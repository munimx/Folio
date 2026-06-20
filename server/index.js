import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import express from 'express'
import pg from 'pg'

const { Pool } = pg

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const rootDir = path.resolve(__dirname, '..')
const distDir = path.join(rootDir, 'dist')
const port = Number(process.env.PORT || 3000)
const databaseUrl = process.env.DATABASE_URL || ''
const pgConnectionTimeoutMs = Number(process.env.PG_CONNECTION_TIMEOUT_MS || 8000)
const pgQueryTimeoutMs = Number(process.env.PG_QUERY_TIMEOUT_MS || 8000)

function useSsl(connectionString) {
  if (!connectionString) return false
  if (process.env.PGSSLMODE === 'disable') return false
  if (process.env.PGSSL === 'true') return { rejectUnauthorized: false }

  try {
    const host = new URL(connectionString).hostname
    if (host === 'localhost' || host === '127.0.0.1' || host.endsWith('.local')) {
      return false
    }
  } catch {
    return false
  }

  return { rejectUnauthorized: false }
}

const pool = databaseUrl
  ? new Pool({
      connectionString: databaseUrl,
      ssl: useSsl(databaseUrl),
      max: Number(process.env.PG_POOL_SIZE || 5),
      connectionTimeoutMillis: pgConnectionTimeoutMs,
      query_timeout: pgQueryTimeoutMs,
    })
  : null

let schemaReady = false

async function ensureSchema() {
  if (!pool || schemaReady) return

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      text TEXT NOT NULL,
      list_id TEXT NOT NULL DEFAULT 'personal',
      done BOOLEAN NOT NULL DEFAULT FALSE,
      starred BOOLEAN NOT NULL DEFAULT FALSE,
      due_date BIGINT,
      reminder_lead_time INTEGER,
      reminder_at BIGINT,
      recurring TEXT,
      subtasks JSONB NOT NULL DEFAULT '[]'::jsonb,
      note TEXT,
      snoozed_until BIGINT,
      notified_at BIGINT,
      created_at BIGINT NOT NULL,
      updated_at BIGINT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS tasks_user_list_created_idx
      ON tasks (user_id, list_id, created_at DESC);

    CREATE INDEX IF NOT EXISTS tasks_user_created_idx
      ON tasks (user_id, created_at DESC);
  `)

  schemaReady = true
}

function requirePool(_req, res, next) {
  if (!pool) {
    res.status(503).json({
      error: 'postgres_not_configured',
      message: 'DATABASE_URL is not set; Folio is running in PWA/Firebase mode.',
    })
    return
  }
  next()
}

function normalizeTask(row) {
  return {
    id: row.id,
    text: row.text,
    listId: row.list_id,
    done: row.done,
    starred: row.starred,
    dueDate: row.due_date == null ? undefined : Number(row.due_date),
    reminderLeadTime:
      row.reminder_lead_time == null ? undefined : Number(row.reminder_lead_time),
    reminderAt: row.reminder_at == null ? undefined : Number(row.reminder_at),
    recurring: row.recurring,
    subtasks: row.subtasks ?? [],
    note: row.note ?? undefined,
    snoozedUntil: row.snoozed_until == null ? undefined : Number(row.snoozed_until),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
    notifiedAt: row.notified_at == null ? undefined : Number(row.notified_at),
  }
}

function compact(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entryValue]) => entryValue !== undefined),
  )
}

function readUid(req) {
  const fromQuery = typeof req.query.uid === 'string' ? req.query.uid : ''
  const fromBody = typeof req.body?.uid === 'string' ? req.body.uid : ''
  return (fromBody || fromQuery).trim()
}

function validateTaskPatch(input) {
  const patch = {}
  if (typeof input.text === 'string') patch.text = input.text.trim()
  if (typeof input.listId === 'string') patch.listId = input.listId.trim()
  if (typeof input.done === 'boolean') patch.done = input.done
  if (typeof input.starred === 'boolean') patch.starred = input.starred
  if (typeof input.dueDate === 'number' || input.dueDate === null) patch.dueDate = input.dueDate
  if (typeof input.reminderLeadTime === 'number' || input.reminderLeadTime === null) {
    patch.reminderLeadTime = input.reminderLeadTime
  }
  if (typeof input.reminderAt === 'number' || input.reminderAt === null) {
    patch.reminderAt = input.reminderAt
  }
  if (
    input.recurring === 'daily' ||
    input.recurring === 'weekly' ||
    input.recurring === null
  ) {
    patch.recurring = input.recurring
  }
  if (Array.isArray(input.subtasks)) patch.subtasks = input.subtasks
  if (typeof input.note === 'string' || input.note === null) patch.note = input.note
  if (typeof input.snoozedUntil === 'number' || input.snoozedUntil === null) {
    patch.snoozedUntil = input.snoozedUntil
  }
  if (typeof input.notifiedAt === 'number' || input.notifiedAt === null) {
    patch.notifiedAt = input.notifiedAt
  }
  return patch
}

function sqlValue(value) {
  return value === undefined ? null : value
}

function withSchema(handler) {
  return async (req, res, next) => {
    try {
      await ensureSchema()
      await handler(req, res)
    } catch (error) {
      next(error)
    }
  }
}

const app = express()
app.disable('x-powered-by')
app.use(express.json({ limit: '256kb' }))

app.get('/api/health', async (_req, res) => {
  if (!pool) {
    res.json({ ok: true, storage: 'firebase', postgres: false })
    return
  }

  try {
    await ensureSchema()
    await pool.query('SELECT 1')
    res.json({ ok: true, storage: 'postgres', postgres: true })
  } catch (error) {
    console.error('Postgres health check failed:', error)
    res.status(503).json({
      ok: false,
      storage: 'postgres',
      postgres: true,
      error: error instanceof Error ? error.message : 'database unavailable',
    })
  }
})

app.get(
  '/api/tasks',
  requirePool,
  withSchema(async (req, res) => {
    const uid = readUid(req)
    if (!uid) {
      res.status(400).json({ error: 'uid_required' })
      return
    }

    const listId = typeof req.query.listId === 'string' ? req.query.listId : ''
    const params = [uid]
    let where = 'WHERE user_id = $1'

    if (listId && listId !== 'today') {
      params.push(listId)
      where += ` AND list_id = $${params.length}`
    }

    const result = await pool.query(
      `SELECT * FROM tasks ${where} ORDER BY created_at DESC`,
      params,
    )

    res.json({ tasks: result.rows.map(normalizeTask) })
  }),
)

app.post(
  '/api/tasks',
  requirePool,
  withSchema(async (req, res) => {
    const uid = readUid(req)
    const task = validateTaskPatch(req.body?.task ?? req.body ?? {})
    const now = Date.now()
    const id = crypto.randomUUID()
    const text = typeof task.text === 'string' ? task.text : ''

    if (!uid) {
      res.status(400).json({ error: 'uid_required' })
      return
    }
    if (!text) {
      res.status(400).json({ error: 'text_required' })
      return
    }

    const result = await pool.query(
      `
      INSERT INTO tasks (
        id, user_id, text, list_id, done, starred, due_date, reminder_lead_time,
        reminder_at, recurring, subtasks, note, snoozed_until, notified_at,
        created_at, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16)
      RETURNING *
      `,
      [
        id,
        uid,
        text,
        task.listId || 'personal',
        task.done ?? false,
        task.starred ?? false,
        sqlValue(task.dueDate),
        sqlValue(task.reminderLeadTime),
        sqlValue(task.reminderAt),
        sqlValue(task.recurring),
        JSON.stringify(task.subtasks ?? []),
        sqlValue(task.note),
        sqlValue(task.snoozedUntil),
        sqlValue(task.notifiedAt),
        task.createdAt ?? now,
        task.updatedAt ?? now,
      ],
    )

    res.status(201).json({ task: normalizeTask(result.rows[0]) })
  }),
)

app.patch(
  '/api/tasks/:id',
  requirePool,
  withSchema(async (req, res) => {
    const uid = readUid(req)
    const id = req.params.id
    const patch = compact(validateTaskPatch(req.body?.patch ?? req.body ?? {}))

    if (!uid) {
      res.status(400).json({ error: 'uid_required' })
      return
    }

    const fields = {
      text: 'text',
      listId: 'list_id',
      done: 'done',
      starred: 'starred',
      dueDate: 'due_date',
      reminderLeadTime: 'reminder_lead_time',
      reminderAt: 'reminder_at',
      recurring: 'recurring',
      subtasks: 'subtasks',
      note: 'note',
      snoozedUntil: 'snoozed_until',
      notifiedAt: 'notified_at',
    }

    const assignments = []
    const params = []
    for (const [key, column] of Object.entries(fields)) {
      if (!(key in patch)) continue
      params.push(key === 'subtasks' ? JSON.stringify(patch[key]) : patch[key])
      assignments.push(
        `${column} = $${params.length}${key === 'subtasks' ? '::jsonb' : ''}`,
      )
    }

    params.push(Date.now())
    assignments.push(`updated_at = $${params.length}`)
    params.push(id, uid)

    const result = await pool.query(
      `
      UPDATE tasks
      SET ${assignments.join(', ')}
      WHERE id = $${params.length - 1} AND user_id = $${params.length}
      RETURNING *
      `,
      params,
    )

    if (result.rowCount === 0) {
      res.status(404).json({ error: 'task_not_found' })
      return
    }

    res.json({ task: normalizeTask(result.rows[0]) })
  }),
)

app.delete(
  '/api/tasks/:id',
  requirePool,
  withSchema(async (req, res) => {
    const uid = readUid(req)
    if (!uid) {
      res.status(400).json({ error: 'uid_required' })
      return
    }

    await pool.query('DELETE FROM tasks WHERE id = $1 AND user_id = $2', [
      req.params.id,
      uid,
    ])

    res.status(204).end()
  }),
)

if (fs.existsSync(distDir)) {
  app.use('/Folio', express.static(distDir, { index: false }))
  app.use(express.static(distDir, { index: false }))
  app.get(['/Folio', '/Folio/'], (_req, res) => {
    res.sendFile(path.join(distDir, 'index.html'))
  })
  app.use((req, res) => {
    if (req.path.startsWith('/api/')) {
      res.status(404).json({ error: 'not_found' })
      return
    }
    res.sendFile(path.join(distDir, 'index.html'))
  })
}

app.use((error, _req, res, _next) => {
  console.error(error)
  res.status(500).json({
    error: 'internal_error',
    message: error instanceof Error ? error.message : 'unknown error',
  })
})

app.listen(port, () => {
  const mode = pool ? 'postgres' : 'firebase/static'
  console.log(`Folio listening on :${port} (${mode})`)
})
