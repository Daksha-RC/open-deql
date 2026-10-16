# Open-Deql — project rules

## Build & workflow

- Build with `cargo build` (debug). Never use `--release` unless explicitly asked.
- Skip UI build steps (`npm run build`) when working on backend tasks.
- `.env` in the project root overrides process env; check it before running the server.
- Before finishing a task: run `cargo fmt --all` and make the CI clippy command
  pass: `cargo clippy --workspace --all-targets -- -W clippy::too_many_lines
  -W clippy::cognitive_complexity -W clippy::excessive_nesting -D warnings`.

## Frontend (web/)

- After every change under `web/`, run `npm run format:check` from `web/`
  before considering the task done. If it flags a file, fix it with
  `npx prettier --write <file>` (not a full-repo `--write`) and re-run
  `format:check` to confirm it's clean.

## Rust code organization

Item order inside a file/module, top to bottom (clippy's default grouping):
`mod`/`pub mod` declarations → `use` imports → macros → `const`/`static` → types (`struct`/`enum`/`trait`/`type`), each immediately followed by its own `impl` blocks (inherent impl first, then trait impls) → functions.

- A type and its `impl` blocks stay together. Do not collect all types at the top and all
  `impl` blocks below them; a reader must see a struct's methods right under its fields.
  Types that only serve another type (private helper structs/enums) go after the type that
  uses them, again followed by their own impls. No lint enforces adjacency, review does.

- Function length, cognitive complexity, and nesting depth are capped by the
  thresholds in clippy.toml (set just above the worst existing code). They only
  ratchet down — if CI flags your function, split it instead of raising the
  threshold.
- Never insert a function above the file's constants or type definitions.
- Keep `pub` and private functions grouped. Do not drop a private helper into the
  middle of the pub API block, or a pub fn into a run of private helpers. Place
  new functions next to related functions within the correct group.
- `#[cfg(test)] mod tests` is always the LAST item in a file. CI enforces this
  (clippy `items_after_test_module` runs under `--all-targets -- -D warnings`).
- The module-level order above matches clippy's `arbitrary_source_item_ordering`
  lint with the `source-item-ordering = ["module"] config in clippy.toml. The
  lint is opt-in (too much pre-existing debt to enable repo-wide); hold the code
  you add to this order even though nothing enforces it yet.

## Comments

STRICT, and enforced in review — a comment that breaks these is deleted, not
reworded:

- **One line, or none.** A comment is at most a single line. No multi-line
  comment blocks in code (the license header is the only exception). If the
  point needs more than one line, it does not belong in a comment.
- **Only logical comments — explain WHY.** The single allowed reason to write a
  comment is to state a non-obvious constraint, invariant, or gotcha the code
  itself cannot express. If a comment is not explaining logic, do not write it;
  no comment is better than a filler one.
- **Banned outright** (remove on sight): styling/visual narration (layout,
  spacing, `px`, "matches the header height"), change/modification notes ("now
  we...", "changed to...", "was X"), and narration that restates what the code
  plainly does.
- `///` / JSDoc doc comments on public APIs: one summary sentence, and only when
  the behaviour is non-obvious; never to restate the signature.

## Version Control

This project uses jj (Jujutsu). 
Use Jujutsu (jj) via jj-mcp-server only; never run jj from the shell. Only call MCP tools with explicit limit, size, max, or equivalent output bounds. Never invoke unbounded show, diff, history, or file-content operations.
Dont use `git`

## Spec-driven development

This project uses OpenSpec for spec-driven development. Follow the staged workflow:
**Explore → New → Update → Continue → Apply → Verify → Sync → Archive**.
The corresponding `openspec-*` skills handle each stage.

## Enterprise-gated code

- Default features do NOT compile `#[cfg(feature = "enterprise")]` code, so a
  green local check proves nothing about it, and `--features enterprise` does
  not work in this repo (the local `src/enterprise/*` stubs don't provide it).
  With access to the o2-enterprise repo, verify by swapping in its
  `Cargo.toml.openobserve` and running `cargo check --all-targets`, then
  restore `Cargo.toml` (never commit the swap). Without access, say so in the PR.
- A change needing paired PRs in openobserve and o2-enterprise must use the
  SAME branch name in both repos — CI checks out the paired side by branch name.
- When changing this repo's root `Cargo.toml`, mirror the change into
  `Cargo.toml.openobserve` in the o2-enterprise repo.

## Tooling

### GitNexus MCP

Use GitNexus for architecture, code-organization, and impact questions. It provides a
knowledge graph of the codebase but is on a remote server that may lag local changes.

| When to use | Tool |
|---|---|
| "How does X work?" | `query()` |
| "What does this symbol do?" | `context()` |
| "What breaks if I change X?" | `impact()` |
| Before modifying API routes | `api_impact()` |
| Before committing | `detect_changes()` |
| Structural queries | `cypher()` |

⚠️ **Staleness caveat:** GitNexus is a remote server. Its index may lag local code by one
or more commits. Always cross-reference with actual files when the result is critical.

### Internet Search

Never make direct internet connections (fetch, curl, `web_url_read` on arbitrary URLs).
**Always use the `searxng_web_search`, `searxng_search_suggestions`. Use suggestions to refine
vague queries before searching.

- **Always append the current date and timezone to search queries** (today is 14th October
  2026, IST UTC+5:30) so results reflect recent content.

### Sequential Thinking

### Sequential Thinking

**Tool:** `sequentialthinking`

**Use for:** Multi-step refactors, architecture decisions, complex debugging, planning and design tasks.
**Skip for:** Trivial single-step tasks.

**Parameters:**
- `thought` (string, required): Your current thinking step. Can include analytical steps, revisions, questions, realizations, approach changes, hypothesis generation/verification.
- `nextThoughtNeeded` (boolean, required): Whether another thought step is needed. Set `false` only when truly done with a satisfactory answer.
- `thoughtNumber` (integer, required): Current number in the sequence (e.g., 1, 2, 3).
- `totalThoughts` (integer, required): Estimated total thoughts needed (can be adjusted up/down as you progress).
- `isRevision` (boolean, optional): Whether this thought revises previous thinking.
- `revisesThought` (integer, optional): Which thought number is being reconsidered (used when `isRevision` is `true`).
- `branchFromThought` (integer, optional): Branching point thought number (used when branching into a new path).
- `branchId` (string, optional): Identifier for the current branch.
- `needsMoreThoughts` (boolean, optional): If reaching the end but realizing more thoughts are needed.

**Capabilities:**
- Dynamic and reflective problem-solving through adjustable thinking steps
- Build on, question, or revise previous insights
- Add more thoughts even after reaching the "end"
- Express uncertainty and explore alternative approaches
- Generate and verify solution hypotheses
- Branch and backtrack as needed
- Final output: a single, ideally correct answer
