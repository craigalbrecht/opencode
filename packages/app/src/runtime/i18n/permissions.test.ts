import { describe, expect, test } from "bun:test"
import en from "./en"

// The permission dock resolves `settings.permissions.tool.<action>.description` and renders an
// empty body when the key does not resolve (session-permission-dock.tsx). Locale dictionaries are
// built as `{ ...english, ...locale }`, so a key present here covers every locale.
const ACTIONS = [
  "read",
  "edit",
  "glob",
  "grep",
  "list",
  "lsp",
  // v2 emits `shell`; v1's `bash` is migrated in core/src/v1/config/migrate.ts.
  "shell",
  // emitted by the question tool plugin
  "question",
  "external_directory",
  "task",
  "skill",
  "todowrite",
  "webfetch",
  "websearch",
]

describe("permission tool descriptions", () => {
  test("exist for every action the app can receive", () => {
    const strings = en as Record<string, string>
    const missing = ACTIONS.flatMap((action) =>
      ["title", "description"]
        .filter((field) => strings[`settings.permissions.tool.${action}.${field}`] === undefined)
        .map((field) => `${action}.${field}`),
    )
    expect(missing).toEqual([])
  })

  test("resolve to non-empty strings, not to the key itself", () => {
    const strings = en as Record<string, string>
    for (const action of ACTIONS) {
      const key = `settings.permissions.tool.${action}.description`
      expect(strings[key]).toBeString()
      expect(strings[key]).not.toBe(key)
      expect(strings[key]!.length).toBeGreaterThan(0)
    }
  })
})
