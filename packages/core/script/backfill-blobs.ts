#!/usr/bin/env bun
/**
 * Moves inline base64 media in existing `session_message.data` and `session_inbox.payload` rows into `blob` and
 * records their `blob_ref` rows, then optionally compacts the database into a new file. Byte-preserving: blobs hold
 * exactly the decoded bytes that were inline, so hydrated reads return the same data as before. Safe to re-run:
 * rows that already hold `media:<hash>` refs are left alone apart from making sure their `blob_ref` rows exist.
 *
 * Requires the `blob` migration to have run (start OpenCode once on the database); the script never creates tables
 * and exits with an error otherwise.
 *
 * Locking and disk space:
 * - Each batch takes a write lock with `BEGIN IMMEDIATE` and a short `busy_timeout` (1000ms by default), so it fails
 *   fast if a running OpenCode server or another writer holds the lock.
 * - `--vacuum-into <out.db>` writes a compacted copy of the whole database and needs free disk space roughly equal to
 *   the current size of `<opencode.db>`.
 *
 * Usage:
 *   bun packages/core/script/backfill-blobs.ts <path/to/opencode.db> [--vacuum-into <out.db>]
 */
import { Database } from "bun:sqlite"
import blobMigration from "../src/database/migration/20261025123711_blob.js"
import { SessionBlob } from "../src/session/blob.js"

export const MAX_BATCH_ROWS = 25
export const MAX_BATCH_BYTES = 50 * 1024 * 1024
export const DEFAULT_BUSY_TIMEOUT_MS = 1_000

type Data = Parameters<typeof SessionBlob.split>[0]
type Blob = ReturnType<typeof SessionBlob.split>["blobs"][number]

export class MigrationMissingError extends Error {
  constructor() {
    super(
      `The ${blobMigration.id} migration has not run on this database. ` +
        "Start OpenCode once against it to apply migrations, then re-run the backfill.",
    )
  }
}

function requireBlobMigration(db: Database) {
  const journal = db
    .query<{ name: string }, []>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration'`)
    .get()
  const applied =
    journal && db.query<{ id: string }, [string]>(`SELECT id FROM migration WHERE id = ?`).get(blobMigration.id)
  if (!applied) throw new MigrationMissingError()
}

// Rows were written by OpenCode from these same types, so their JSON is the stored shape.
const parse = (json: string): Data => JSON.parse(json)

type PreparedItem = {
  readonly id: string
  readonly sessionID: string
  readonly seq: number
  readonly originalJson: string
  readonly json: string | undefined
  readonly blobs: ReadonlyArray<Blob>
  readonly hashes: ReadonlyArray<string>
}

export async function backfillBlobs(
  db: Database,
  options?: {
    readonly vacuumInto?: string
    readonly now?: number
    readonly busyTimeoutMs?: number
    readonly maxBatchRows?: number
    readonly maxBatchBytes?: number
    readonly onBeforeBatchCommit?: (ids: ReadonlyArray<string>) => void
  },
) {
  requireBlobMigration(db)

  const busyTimeoutMs = options?.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS
  const maxBatchRows = options?.maxBatchRows ?? MAX_BATCH_ROWS
  const maxBatchBytes = options?.maxBatchBytes ?? MAX_BATCH_BYTES
  const now = options?.now ?? Date.now()

  db.exec(`PRAGMA busy_timeout = ${Math.max(0, Math.floor(busyTimeoutMs))};`)

  const insertBlob = db.prepare(
    `INSERT OR IGNORE INTO blob (hash, mime, bytes, width, height, data, time_created) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
  // A ref to a blob that is already gone has nothing to keep alive (blob_ref.hash references blob).
  const upsertRef = db.prepare(
    `INSERT INTO blob_ref (session_id, message_id, hash, seq)
     SELECT ?1, ?2, ?3, ?4 WHERE EXISTS (SELECT 1 FROM blob WHERE hash = ?3)
     ON CONFLICT(session_id, message_id, hash) DO UPDATE SET seq = excluded.seq`,
  )

  let candidates = 0
  let migratedRows = 0
  let skippedChanged = 0
  let blobsStored = 0
  let refsRecorded = 0

  const backfillTable = (
    table: "session_message" | "session_inbox",
    column: "data" | "payload",
    seqColumn: "seq" | "enqueued_seq",
  ) => {
    const ids = db
      .query<
        { id: string },
        []
      >(`SELECT id FROM ${table} WHERE ${column} LIKE '%;base64,%' OR ${column} LIKE '%"files"%' OR ${column} LIKE '%"media:%'`)
      .all()
    candidates += ids.length

    const selectRow = db.prepare<{ session_id: string; seq: number; json: string }, [string]>(
      `SELECT session_id, ${seqColumn} AS seq, ${column} AS json FROM ${table} WHERE id = ?`,
    )
    const updateRow = db.prepare(`UPDATE ${table} SET ${column} = ? WHERE id = ? AND ${column} = ?`)

    const commitBatch = db.transaction((items: ReadonlyArray<PreparedItem>) => {
      for (const item of items) {
        const current = selectRow.get(item.id)
        if (
          !current ||
          current.json !== item.originalJson ||
          current.seq !== item.seq ||
          current.session_id !== item.sessionID
        ) {
          skippedChanged++
          continue
        }
        if (item.json !== undefined) {
          if (updateRow.run(item.json, item.id, item.originalJson).changes === 0) {
            skippedChanged++
            continue
          }
          migratedRows++
        }
        for (const blob of item.blobs)
          blobsStored += insertBlob.run(
            blob.hash,
            blob.mime,
            blob.bytes,
            blob.width ?? null,
            blob.height ?? null,
            blob.data,
            now,
          ).changes
        for (const hash of item.hashes) refsRecorded += upsertRef.run(item.sessionID, item.id, hash, item.seq).changes
      }
    })

    let batch: PreparedItem[] = []
    let batchBytes = 0
    const flush = () => {
      if (batch.length === 0) return
      options?.onBeforeBatchCommit?.(batch.map((item) => item.id))
      commitBatch.immediate(batch)
      batch = []
      batchBytes = 0
    }

    for (const { id } of ids) {
      // Decode and hash outside the write transaction so the lock is only held for fast batch writes.
      const row = selectRow.get(id)
      if (!row) continue
      const split = SessionBlob.split(parse(row.json))
      if (!split.moved && split.hashes.size === 0) continue

      const itemBytes = split.blobs.reduce((sum, blob) => sum + blob.bytes, 0)
      if (batch.length > 0 && batchBytes + itemBytes > maxBatchBytes) flush()
      batch.push({
        id,
        sessionID: row.session_id,
        seq: row.seq,
        originalJson: row.json,
        json: split.moved ? JSON.stringify(split.data) : undefined,
        blobs: split.blobs,
        hashes: [...split.hashes],
      })
      batchBytes += itemBytes
      if (batch.length >= maxBatchRows || batchBytes >= maxBatchBytes) flush()
    }
    flush()
  }

  backfillTable("session_message", "data", "seq")
  backfillTable("session_inbox", "payload", "enqueued_seq")

  if (options?.vacuumInto) db.exec(`VACUUM INTO '${options.vacuumInto.replace(/'/g, "''")}';`)

  return {
    candidates,
    migratedRows,
    skippedChanged,
    blobsStored,
    refsRecorded,
    vacuumTarget: options?.vacuumInto,
  }
}

if (import.meta.main) {
  const args = process.argv.slice(2)
  const vacuumIdx = args.indexOf("--vacuum-into")
  const vacuumInto = vacuumIdx !== -1 ? args[vacuumIdx + 1] : undefined
  const dbPath = args.find((arg, index) => !arg.startsWith("--") && (vacuumIdx === -1 || index !== vacuumIdx + 1))

  if (!dbPath) {
    console.error(
      [
        "Usage: bun packages/core/script/backfill-blobs.ts <opencode.db> [--vacuum-into <out.db>]",
        "Note: --vacuum-into writes a full compacted copy and requires free disk space roughly equal to <opencode.db>.",
      ].join("\n"),
    )
    process.exit(1)
  }

  const db = new Database(dbPath)
  try {
    console.log(JSON.stringify(await backfillBlobs(db, { vacuumInto })))
  } catch (error) {
    if (!(error instanceof MigrationMissingError)) throw error
    console.error(error.message)
    process.exitCode = 1
  } finally {
    db.close()
  }
}
