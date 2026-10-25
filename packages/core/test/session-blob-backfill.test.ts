import { describe, expect, test } from "bun:test"
import { Database as Sqlite } from "bun:sqlite"
import path from "path"
import { asc, sql } from "drizzle-orm"
import { DateTime, Effect, Schema } from "effect"
import { Global } from "@opencode/util/global"
import { Agent } from "@opencode/core/agent"
import { Database } from "@opencode/core/database/database"
import { Model } from "@opencode/core/model"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionBlob } from "@opencode/core/session/blob"
import { SessionMessage } from "@opencode/core/session/message"
import {
  BlobRefTable,
  BlobTable,
  SessionInboxTable,
  SessionMessageTable,
  SessionTable,
} from "@opencode/core/session/sql"
import { SessionStored } from "@opencode/core/session/stored"
import blobMigration from "../src/database/migration/20261025123711_blob"
import { backfillBlobs, MigrationMissingError } from "../script/backfill-blobs"
import { tmpdir } from "./fixture/tmpdir"

type DatabaseService = Database.Interface["db"]

const sessionID = Session.ID.make("ses_backfill")
const pdf = Buffer.from("legacy-inline-attachment-bytes")
const png = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
  "base64",
)
const files = [
  { data: pdf.toString("base64"), mime: "application/pdf", source: { type: "inline" as const }, name: "legacy.pdf" },
  { data: png.toString("base64"), mime: "image/png", source: { type: "inline" as const }, name: "legacy.png" },
]

const assistant = () => {
  const {
    id: _,
    type,
    ...data
  } = Schema.encodeSync(SessionMessage.Info)(
    SessionMessage.Assistant.make({
      id: SessionMessage.ID.make("msg_assistant"),
      type: "assistant",
      agent: Agent.ID.make("build"),
      model: { id: Model.ID.make("model"), providerID: Provider.ID.make("provider") },
      content: [
        {
          type: "tool",
          id: "call_1",
          name: "read",
          executed: true,
          state: {
            status: "completed",
            input: {},
            content: [
              { type: "file", uri: `data:application/pdf;base64,${pdf.toString("base64")}`, mime: "application/pdf" },
            ],
          },
          time: { created: DateTime.makeUnsafe(0), ran: DateTime.makeUnsafe(0), completed: DateTime.makeUnsafe(0) },
        },
      ],
      time: { created: DateTime.makeUnsafe(0) },
    }),
  )
  return { type, data }
}

const withDatabase = <A>(filename: string, use: (db: DatabaseService) => Effect.Effect<A>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      return yield* use(db)
    }).pipe(
      Effect.provide(Database.layer({ path: filename })),
      Effect.provideService(Global.Service, Global.make({ data: path.dirname(filename) })),
    ),
  )

const seed = (db: DatabaseService) =>
  Effect.gen(function* () {
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "s",
        directory: "/project",
        title: "t",
        version: "v",
      })
      .run()
    const tool = assistant()
    yield* db
      .insert(SessionMessageTable)
      .values([
        {
          id: SessionMessage.ID.make("msg_user"),
          session_id: sessionID,
          type: "user",
          seq: 1,
          time_created: 0,
          data: { text: "legacy user", files, time: { created: 0 } },
        },
        { id: SessionMessage.ID.make("msg_assistant"), session_id: sessionID, seq: 2, time_created: 0, ...tool },
      ])
      .run()
    yield* db
      .insert(SessionInboxTable)
      .values({
        id: SessionMessage.ID.make("msg_inbox"),
        session_id: sessionID,
        type: "user",
        payload: { text: "legacy queued", files },
        delivery: "queue",
        enqueued_seq: 3,
        time_created: 0,
      })
      .run()
  }).pipe(Effect.orDie)

/** Rows as the read path returns them, with any refs hydrated back to inline bytes. */
const readHydrated = (db: DatabaseService) =>
  Effect.gen(function* () {
    const messages = yield* db.select().from(SessionMessageTable).orderBy(asc(SessionMessageTable.seq)).all()
    const inbox = yield* db.select().from(SessionInboxTable).all()
    return {
      messages: (yield* SessionBlob.hydrate(db, messages)).map((row) => row.data),
      inbox: (yield* SessionBlob.hydrateInbox(db, inbox)).map((row) => row.payload),
    }
  }).pipe(Effect.orDie)

const rawRows = (sqlite: Sqlite) =>
  JSON.stringify([
    sqlite.query<{ data: string }, []>(`SELECT data FROM session_message ORDER BY seq`).all(),
    sqlite.query<{ payload: string }, []>(`SELECT payload FROM session_inbox`).all(),
  ])

describe("backfillBlobs", () => {
  test("moves legacy inline media into blobs byte-for-byte, records refs, and is idempotent", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "opencode.db")
    const before = await withDatabase(filename, (db) => seed(db).pipe(Effect.andThen(readHydrated(db))))

    const sqlite = new Sqlite(filename)
    try {
      const first = await backfillBlobs(sqlite, { now: 123, maxBatchRows: 1 })
      expect(first).toMatchObject({ migratedRows: 3, skippedChanged: 0, blobsStored: 2, refsRecorded: 5 })
      expect(rawRows(sqlite)).not.toContain(";base64,")
      expect(rawRows(sqlite)).toContain(SessionStored.ref(SessionBlob.hashBytes(pdf)))

      const second = await backfillBlobs(sqlite, { now: 456 })
      expect(second).toMatchObject({ migratedRows: 0, skippedChanged: 0, blobsStored: 0 })
    } finally {
      sqlite.close()
    }

    await withDatabase(filename, (db) =>
      Effect.gen(function* () {
        const blobs = yield* db.select().from(BlobTable).all().pipe(Effect.orDie)
        const byHash = new Map(blobs.map((blob) => [blob.hash, blob]))
        for (const [original, mime] of [
          [pdf, "application/pdf"],
          [png, "image/png"],
        ] as const) {
          const blob = byHash.get(SessionBlob.hashBytes(original))
          expect(blob).toMatchObject({ mime, bytes: original.byteLength })
          expect(Buffer.from(blob?.data ?? []).equals(original)).toBe(true)
        }

        const refs = yield* db.select().from(BlobRefTable).orderBy(asc(BlobRefTable.seq)).all().pipe(Effect.orDie)
        expect(refs.map((ref): [string, number] => [ref.message_id, ref.seq])).toEqual([
          ["msg_user", 1],
          ["msg_user", 1],
          ["msg_assistant", 2],
          ["msg_inbox", 3],
          ["msg_inbox", 3],
        ])
        expect(yield* readHydrated(db)).toEqual(before)
      }),
    )
  })

  test("refuses to run before the blob migration and leaves the database untouched", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "opencode.db")
    await withDatabase(filename, (db) =>
      Effect.gen(function* () {
        yield* seed(db)
        yield* db.run(sql`DROP TABLE blob_ref`)
        yield* db.run(sql`DROP TABLE blob`)
        yield* db.run(sql`DELETE FROM migration WHERE id = ${blobMigration.id}`)
      }).pipe(Effect.orDie),
    )

    const sqlite = new Sqlite(filename)
    try {
      const before = rawRows(sqlite)
      const refused = await backfillBlobs(sqlite).then(
        () => undefined,
        (error: unknown) => error,
      )
      expect(refused).toBeInstanceOf(MigrationMissingError)
      expect(String(refused)).toContain(blobMigration.id)
      expect(rawRows(sqlite)).toBe(before)
    } finally {
      sqlite.close()
    }
  })

  test("skips a row that changes before its batch commits, and fails fast when another writer holds the lock", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "opencode.db")
    await withDatabase(filename, seed)

    const sqlite = new Sqlite(filename)
    try {
      const raced = await backfillBlobs(sqlite, {
        onBeforeBatchCommit: (ids) => {
          if (ids.includes("msg_user"))
            sqlite
              .query(`UPDATE session_message SET data = json_set(data, '$.text', 'concurrently updated') WHERE id = ?`)
              .run("msg_user")
        },
      })
      expect(raced.skippedChanged).toBe(1)
      const row = sqlite.query<{ data: string }, []>(`SELECT data FROM session_message WHERE id = 'msg_user'`).get()
      expect(row?.data).toContain("concurrently updated")
      expect(row?.data).toContain(files[0]!.data)

      const locker = new Sqlite(filename)
      try {
        locker.exec("BEGIN IMMEDIATE;")
        const locked = await backfillBlobs(sqlite, { busyTimeoutMs: 25 }).then(
          () => undefined,
          (error: unknown) => error,
        )
        expect(String(locked)).toContain("locked")
      } finally {
        locker.close()
      }
    } finally {
      sqlite.close()
    }
  })
})
