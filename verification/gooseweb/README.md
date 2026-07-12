# Gooseweb acceptance contract

This directory is the source-controlled contract for Golden Goose migration browser review. `schemas/` versions the record shapes, `manifests/` describes product scenarios, `ledger/phase-state.json` is the exact P00-P56 graph and state history, and `allowlists/` is the complete set of expected browser messages and requests. Validation is deterministic and runs in the existing Gooseweb test entrypoint.

Evidence belongs at `tmp/gg/gooseweb-migration/<phase-id>/<sha7>/attempt-<n>/` and must match `schemas/evidence-run.schema.json`. It is intentionally not committed. Each run contains a manifest copy and SHA-256, environment descriptor, screenshots, console/network/WebSocket results, redacted runtime/Tower/store state, check results, applicable metrics, report, and exact-head clearance JSON.

Never retain credentials, cookies, CSRF values, bearer tokens, realtime tickets or query secrets, provider authentication, raw image bytes, or secret configuration. Redact at capture time; a later cleanup is not an acceptable control.

Browser acceptance uses one uniquely named real-Chromium `agent-browser` session. Only the supervisor may own the migration-wide stack lease and lifecycle. Reviewers are read-only and own only browser interaction and evidence.
