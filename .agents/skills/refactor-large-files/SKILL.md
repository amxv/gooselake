---
name: refactor-large-files
description: Mechanically reduce LOC count across large Rust files to stay below 1000 while preserving external module paths and behavior.
---

First, build the current Rust large-file checklist:

```bash
find crates sidecars/gg-mcp-server -path '*/target' -prune -o -type f -name '*.rs' -print | xargs wc -l | sort -nr
```

Work through every file above 1000 LOC one at a time.

For each target, spawn a Codex agent and give it a prompt similar to this:

```text
You are working in `/Users/ashray/code/amxv/gooselake`.

Task:
Refactor `${TARGET_RUST_FILE}` into multiple Rust files so every resulting file is under 1000 LOC, using a safe mechanical refactor that does not require other code to change imports.

Primary goal:
Reduce size and complexity while preserving exact behavior.

Mandatory first reads (in this order):
1. `${TARGET_RUST_FILE}` (read fully in chunks)
2. Any directly related tests/modules that reference `${TARGET_RUST_FILE}`
3. `AGENTS.md`

Hard constraints:
1. Keep external API/import stability for existing callers.
2. If `${TARGET_RUST_FILE}` is `foo.rs`, prefer `foo.rs -> foo/mod.rs + submodules` to preserve module path.
3. Keep `mod.rs` as the stable boundary with re-exports when needed.
4. Avoid semantic changes during extraction; prioritize move-only/mechanical edits.
5. Keep visibility minimal (`pub(super)`/`pub(crate)` where possible, `pub` only when required).
6. No destructive git commands.
7. No sub-agents.
8. For long-running commands such as Cargo checks/tests, use `gg_process_run`, then end the turn immediately and wait for the auto completion injection. Never poll with `gg_process_status`.

Systematic process:
1. Baseline
   1. Record current LOC and public API surface of `${TARGET_RUST_FILE}`.
   2. Identify cohesive responsibility boundaries from the existing code.

2. Mechanical extraction
   1. Convert to `mod.rs` structure when applicable.
   2. Move code into focused submodules incrementally.
   3. Keep each created/edited Rust file under 1000 LOC.
   4. Preserve behavior and external signatures.

3. Test alignment
   1. Update internal module paths/tests only as needed for compilation.
   2. Do not change behavior assertions at all.
   3. If touched Rust test files remain over 1000 LOC, split them safely too.

4. Validation
   1. Run targeted Rust checks/tests while extracting.
   2. Verify all files touched in this refactor are under 1000 LOC.

5. Report
   1. Final module tree.
   2. LOC per resulting file.
   3. Explicit statement on behavior changes (expected: none).
   4. Exact files changed and why.
   5. Any unavoidable external call-site edits and rationale (expected: none).

Definition of done:
1. `${TARGET_RUST_FILE}` is decomposed into coherent modules under 1000 LOC each.
2. Existing external imports/callers do not need to change.
3. Behavior is preserved.
```

Session procedure for the lead:

1. Pick the next Rust file over 1000 LOC from the checklist.
2. Spawn one Codex agent with `gg_team_manage` using `model_preset: "codex"`.
3. Put the full onboarding/refactor prompt into `gg_team_manage.prompt`; do not send a duplicate follow-up DM.
4. Check free space. If it is below 7 GB, run `cargo clean` in the specific crate/sidecar directory you are about to work in.
5. Wait for the agent to report completion.
6. Run the relevant Rust validation locally. Prefer targeted checks first, then broader repo checks if needed.
7. If checks fail, send the concrete errors back to the same agent.
8. Once checks pass, commit and push only if explicitly requested by the user.
9. Mark the checklist item complete locally and move to the next file sequentially.

Sequencing rules:

- Work sequentially, never in parallel.
- Do not ask whether to continue to the next file unless the user explicitly pauses or redirects.
- Agents must not edit the checklist artifact; the lead owns checklist updates.
- This Gooselake adaptation is Rust-only. Ignore docs-site and other non-Rust files for this workflow.
