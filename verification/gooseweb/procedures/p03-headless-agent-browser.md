# P03 headless `agent-browser` acceptance procedure

This is a reviewer-operated procedure, not a browser runner. It defines how the installed `agent-browser` CLI and the user's real local Google Chrome/Chromium produce one repeatable P03 acceptance/evidence attempt. Do not convert it into a shell script, browser config, package dependency, CI job, downloaded browser, persistent browser profile, or alternate automation framework.

The approved plan hash is `521073ac7551df15d814b1e84d1be47ec9e80289728d07c3dbab8c5b2b1b3b2c`. P03 is verification-infrastructure-only: `product_approved` remains `false`, `known_defects` remains empty, and the ten finite P01/P02 `baseline_detected` entries remain mapped to P06-P10.

## 1. Authority and attachment preflight

The reviewer receives the URL and immutable attempt tuple from the lease-holding supervisor. The tuple must include phase/attempt, base/range/head/tree, served head/tree, branch and clean-tree proof, lease ID/sequence and prior-lease termination evidence, `DEV_DIR`, three ports, source configuration, stack mode, managed-process identity, and start timestamp. The reviewer must not start, stop, restart, configure, build, or otherwise control the stack. A reviewer may attach only after the supervisor confirms the prior lease terminated and the P03 clean head owns the exclusive migration slot.

Reject the attempt before opening Chrome when any of the following is true:

- candidate and served head/tree differ, the tree is dirty, or hot reload supplied the candidate;
- the URL was not supplied by the supervisor or contains any query value, including a Gooseweb fixture flag or realtime ticket;
- `AGENT_BROWSER_HEADED` is present with any truthy value;
- `~/.agent-browser/config.json`, repository `agent-browser.json`, or `AGENT_BROWSER_CONFIG` resolves to a config with `headed: true`;
- `AGENT_BROWSER_ENGINE` is set to anything except `chrome`, a cloud provider/CDP attachment is requested, or the executable is not the recorded user-installed Google Chrome/Chromium binary;
- a persistent `--profile`, `--state`, or `--session-name` is proposed; or
- another browser framework, browser binary/cache installer, or browser runner is involved.

Record only the existence and safe result of the checks. Never copy config contents, environment secrets, query values, tickets, or credentials into evidence.

## 2. Browser identity, uniqueness, and verified headless mode

Read the local browser version without starting a browser:

```text
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --version
```

The normal macOS binary is `/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`; an actual user-installed Chromium path is also permitted when recorded. Chrome for Testing, bundled/downloaded browsers, Lightpanda, Firefox, WebKit, remote/cloud providers, and an auto-connected existing Chrome are not accepted.

Construct a new session name for every attempt:

```text
gooseweb-p03-<sha7>-a<attempt>-<reviewer-slug>-<8-random-hex>
```

The name must be absent from the attempt/lifecycle history and `agent-browser session list`. Do not use the default session and never use `close --all`.

Open the supervisor URL with all launch choices explicit:

```text
agent-browser --session <unique-name> --engine chrome --headed false --executable-path <recorded-local-chrome> open <supervisor-url>
```

Immediately record the `agent-browser --version`, executable path/full `--version`, session name, CLI `--headed false`, engine, `navigator.userAgent`, `navigator.webdriver`, `window.devicePixelRatio`, and initial URL with query values removed. Current Chrome deliberately reduces the headless UA version: local Chrome `150.0.7871.115` reports `HeadlessChrome/150.0.0.0`. The required UA proof is therefore a typed `reduced_major_only` four-part token whose minor/build/patch are exactly zero and whose major exactly equals the separately recorded full binary-version major. A full-version UA token, malformed token, `Chrome/` without `HeadlessChrome/`, or different major fails. `navigator.webdriver` must be the typed boolean `true`; a string or omitted value is not proof.

When supported, evaluate `navigator.userAgentData.getHighEntropyValues(["fullVersionList"])` through the same named `agent-browser` session and record the typed brand/version entries. At least one Chrome/Chromium brand must exist; every real Chrome/Chromium brand version must exactly equal the recorded local binary full version, and a Google Chrome executable must include a `Google Chrome` brand. High-entropy evidence is supplemental because `navigator.userAgentData` is not portable across every supported Chrome context. When the API is unavailable, record `availability: unavailable`, an empty list, and a specific nonempty reason. The fallback never weakens the enforceable tuple: explicit local Chrome/Chromium executable path + full binary `--version` + reduced `HeadlessChrome/<same-major>.0.0.0` + `navigator.webdriver: true` + explicit headless/config/environment/session proofs. The P03 descriptor records `execution_mode: headless`, `headed_cli_value: false`, `headed_environment: absent`, `headed_config: absent`, `real_local_chromium: true`, and `persistent_state_loaded: false`. Any mismatch ends the attempt.

## 3. Fresh-context and stale-context proof

Before the first product action, generate an attempt nonce and store it only in the evidence descriptor. Evaluate the following through `agent-browser eval --stdin`, adapting only the nonce literal:

```js
const prior = sessionStorage.getItem("gooseweb-p03-context-nonce");
sessionStorage.setItem("gooseweb-p03-context-nonce", "<attempt-nonce>");
({ prior, current: sessionStorage.getItem("gooseweb-p03-context-nonce") });
```

`prior` must be the typed JSON value `null` and is stored as `initial_prior_context_nonce`; omission, an empty string, or any prior value is stale-context evidence and fails. Inspect `agent-browser storage local` and cookies only to establish emptiness; never retain values. If the login/dev-ticket flow creates state, record names/counts only after capture-time redaction.

For disposal, use only the named session. First evaluate origin cleanup, then clear supported storage and close the session:

```js
await Promise.all((await indexedDB.databases()).filter((db) => db.name).map((db) => new Promise((resolve, reject) => {
  const request = indexedDB.deleteDatabase(db.name);
  request.onsuccess = () => resolve(db.name);
  request.onerror = () => reject(request.error);
  request.onblocked = () => reject(new Error(`blocked IndexedDB deletion: ${db.name}`));
})));
localStorage.clear();
sessionStorage.clear();
await Promise.all((await caches.keys()).map((name) => caches.delete(name)));
await Promise.all((await navigator.serviceWorker?.getRegistrations?.() ?? []).map((registration) => registration.unregister()));
true;
```

Then run `agent-browser --session <name> cookies clear` and `agent-browser --session <name> close`. Create a second, globally unique session with the same explicit headless/engine/executable arguments. Its nonce probe must again return typed `prior: null`, and the new nonce must differ from the old nonce. Record `old_context_disposed`, IndexedDB/cookie/local/session/CacheStorage clearing, service-worker unregistration, `remaining_cache_names: 0`, `remaining_service_workers: 0`, and `stale_context_detected: false`. Any blocked deletion/unregistration or nonzero remainder fails. Never reuse a name to claim a fresh context.

## 4. Semantic UI workflow

Use an interactive accessibility snapshot, visible roles/names/labels, and fresh refs after every DOM/navigation change. CSS selectors, coordinates, direct store mutation, runtime-only calls, fixture-only controls, or JavaScript-dispatched clicks cannot satisfy the browser action.

The required current Gooseweb controls are:

- an enabled roster `button` whose accessible name includes the visible agent title; select it through `find role button click --name <visible-title>` after confirming the corresponding `data-roster-row` is visible;
- `textbox` named `Agent thread composer`;
- `button` named `Send agent thread message`;
- when Team Comms is exercised, `textbox` named `Team comms composer` and `button` named `Send team comms message`.

The reviewer records the selected roster button's exact accessible name and typed `visible`, `enabled`, and `selected` results. For manifest action `action-send-deterministic-turn`, type the exact canonical literal `P02 deterministic turn action` into the visible composer and submit it exactly once. Do not paraphrase, substitute a fixture-only phrase, or retry. This literal is the active manifest's `expected_submitted_text` and matches the deterministic fake source contract. The P03 descriptor records the active manifest action ID, exact control/submit role and accessible names, submitted text, `browser_submission_count: 1`, `command_count: 1`, and `visible_submission_count: 1`; the validator cross-checks these values and every authority content hash against the active manifest rather than accepting an independent fixture string.

Capture the complete redacted authority chain as four ordered typed entries: Gooselake record/event/cursor in `runtime-state.redacted.json`; Goosetower materialized entity/version and served frame or explicit unavailable evidence in `tower-state.redacted.json`; active `realtime-command-worker.ts` Worker/store state in `store-state.redacted.json`; and rendered text/control state in `screenshots/1440x1000.png`. The three JSON artifacts themselves use `p03-authority-observation/v1`; the validator parses their actual content and cross-checks it against the P03 descriptor, so naming an arbitrary standard file is insufficient. Every layer records the same action correlation ID, its own stable semantic entity identity and cursor/version, and the same SHA-256 of the exact submitted text; this prevents four unrelated layer claims from being presented as one journey. Each entry also records `observed_instances`, `missing_count`, `duplicate_count`, `order_errors`, status, and an optional mapped baseline ID. A fully observed entry requires exactly one instance and zero discrepancies. A baseline divergence requires a measured nonzero discrepancy and a baseline ID present in the active manifest. `first_divergent_layer` is null only when every layer is observed; otherwise one or more layers may be divergent and it must name the earliest divergent entry in the fixed runtime→Tower→Worker/store→React order. Downstream cascade measurements remain explicit rather than being discarded. A later-layer success never repairs an earlier contradiction.

## 5. Viewport and screenshot protocol

Run the exact ordered matrix `1440x1000`, `820x1000`, `520x900`. For each size, set the viewport, re-snapshot, keep the primary workflow visible, and save an unannotated PNG at the exact logical dimensions. Measure and record:

```js
({
  width: innerWidth,
  height: innerHeight,
  horizontalOverflow: document.documentElement.scrollWidth !== document.documentElement.clientWidth,
  composerBottom: document.querySelector('.mission-composer, .mission-team-comms-composer')?.getBoundingClientRect().bottom ?? null,
  primaryActionBottom: document.querySelector('[aria-label="Send agent thread message"], [aria-label="Send team comms message"]')?.getBoundingClientRect().bottom ?? null
});
```

Fail if the viewport differs by one CSS pixel, the PNG dimensions differ, document horizontal overflow exists, the composer/primary action extends outside the viewport, or a critical control is unreachable. When a sheet/dialog is relevant, verify focus entry, Escape close, and focus return.

## 6. Console, network, and WebSocket evidence

Capture after `document.readyState === "complete"` plus one second, and again after actions/reconstruction:

- complete unfiltered `agent-browser console` and `agent-browser errors` output;
- complete unfiltered `agent-browser network requests`, retaining method, redacted path, query-key names only, status, resource type, and same-origin classification;
- WebSocket open/close/error/frame summaries from the P02 redacted supervisor observer alongside the same named browser session. If direct frame capture is unavailable, record that exact limitation and its mapped baseline; do not infer frames.

Retain the complete raw HTTP request sequence across four explicit capture segments in this exact order: `initial_load`, `ordinary_reload`, `hard_reload`, and `fresh_context`. Start each segment before its named browser trigger and end it only after the semantic/observable state used by that reconstruction step is reached. `network.json` uses `gooseweb-network-capture/v4`: its ordered segment descriptors record the exact trigger, context generation, completeness bounds, and derived raw-request count, and every raw HTTP entry carries exactly one `segment_id`. Do not clear, replace, filter, or restart the accumulated capture between segments. The initial, ordinary-reload, and hard-reload segments belong to context generation 1; the second unique ephemeral session is context generation 2.

Within every segment, the retained evaluated traffic must contain exactly one successful same-origin query-free `GET /` document and exactly one successful same-origin query-free `POST /api/dev-ticket`. A segment may contain at most one `GET /favicon.ico` 404 only with `BASE-P01-FAVICON-NOT-FOUND`; its absence is not fabricated. Successful same-origin static stylesheet/font/script/module requests remain in the raw list and are the only entries the validator may omit from the evaluated segment. Missing, extra, duplicated, reordered, filtered, cross-origin, unexpected-status, query-bearing, or segment-misattributed traffic fails.

The existing exact console/network allowlists remain authoritative. P01/P02 keep their aggregate v3 network contract; P03 requires the ordered v4 reconstruction segments above. Any warning/error/exception, unexpected request/status, protocol decode issue, abnormal socket close/retry loop, missing capture, filtering, or hidden failure rejects the descriptor. Cookies, authorization headers, CSRF values, ticket/query values, payload secrets, provider auth, raw image bytes, and secret config are redacted before they leave the capture source.

## 7. Reconstruction sequence

After the initial visible outcome and each DOM change, re-snapshot before interaction.

1. Ordinary reload: `agent-browser --session <name> reload`; wait for an observable control/state, not a fixed sleep; recapture layers and visible cardinality.
2. Hard reload on the required current macOS reference machine: focus the rendered document and run `agent-browser --session <name> press Meta+Shift+R`. This is Chrome's cache-bypassing hard-reload keyboard gesture; an ordinary `reload`, `location.reload(true)`, or clearing only CacheStorage/service workers does not satisfy it. Wait for a semantic control or authoritative visible state to reconstruct, then recapture the full console/network/WebSocket and layer evidence. If the installed `agent-browser`/Chrome combination does not deliver or cannot evidence this gesture, record hard reload as an explicit capability blocker and fail the attempt—do not substitute an external CDP client or alternate browser harness. A future non-macOS reference may use its platform Chrome gesture only after that exact command is independently verified and added to this procedure.
3. Navigation away/back: use visible top-navigation controls to leave Agents, then a visible control to return; browser `back` alone is insufficient if the product navigation is an in-app view switch.
4. Reconnect: use the supervisor-provided one-layer fault control/observer. The reviewer does not restart the stack. Verify honest offline/stale/replaying state and eventual exact-once convergence.
5. Fresh context: complete the disposal procedure, close only the named session, start the new unique session, repeat identity/stale-context proof and the critical visible workflow.

Each reconstruction step is a typed record, not the string `pass`: it names a referenced standard evidence artifact, status, `missing_count`, `duplicate_count`, `order_errors`, and optional mapped baseline ID. A passing step requires all three counts to be zero and no baseline; a divergent step requires a nonzero measured discrepancy and a baseline present in the active manifest. Product failures may remain only as one of the ten finite mapped baselines; infrastructure, leakage, wrong-head, stale-context, headed, non-real-Chromium, or evidence-completeness failures always stop P03.

If the completed supervisor process reports `invalid source lifecycle transition Stale -> Replaying`, retain the exact redacted log/report evidence and classify it under the existing `BASE-P02-LIVE-GAP-TRANSITION-PANIC` / P06 lifecycle-algebra baseline. P03 must not correct, suppress, or relabel that product behavior.

## 8. Fixture-leak and production/default checks

The default development URL and production-build URL must contain no Gooseweb fixture query key, fixture marker/fake entity, or fixture-only global/control. A render fixture may prove appearance only and requires both `import.meta.env.DEV` and its explicit query parameter. The source-backed P02 scenario is the correctness path. Production evidence must use the active real Worker entry, not the development inline core.

Record `query_flags_present: false`, zero fixture markers, and separate `default_development`/`production_build` results. Do not paste query values into evidence. Any fixture leakage is a seeded hard failure.

## 9. Required evidence and completion

Retain the following under `tmp/gg/gooseweb-migration/P03/<sha7>/attempt-<n>/`: manifest copy/hash, standard evidence-run descriptor, `p03-browser-evidence.json`, redacted environment/lease/stack tuple, three screenshots, complete console/network/WebSocket captures, redacted runtime/Tower/store state, checks, procedure-overhead measurement, report, and typed outcome. Validate `p03-browser-evidence.json` against `verification/gooseweb/schemas/p03-browser-evidence.schema.json` and the semantic validator.

P03 infrastructure is complete only when the validator rejects the nine foundational seeded classes—console, network, WebSocket, wrong head, wrong viewport, fixture leak, stale context, headed mode, and non-real Chromium—plus every missing/false/mismatched journey, exact-once cardinality, authority, first-divergence, reconstruction-integrity, automation, initial-nonce, CacheStorage, service-worker, and standard-linkage proof in the P03 smoke suite. The reviewer closes only the named browser session. The supervisor later stops/cleans/releases the stack and the final outcome follows the existing P01 lifecycle/clearance governance. No product approval is issued by P03.
