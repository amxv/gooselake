import {
  applySchemaInline,
  array,
  consoleCaptureSchema,
  equal,
  exactMultiset,
  fail,
  ensureUnique,
  integer,
  multisetSignature,
  networkCaptureSchema,
  object,
  readJson,
  resolveManifestBaseline,
  requireText,
  sameMultiset,
  scanSecrets,
  string,
  type RecordJson
} from "./validate";

export function validateBrowserCaptures(consoleCapture: RecordJson, networkCapture: RecordJson, manifest?: RecordJson): void {
  const consoleAllowlist = readJson("verification/gooseweb/allowlists/console.json");
  const networkAllowlist = readJson("verification/gooseweb/allowlists/network.json");
  applySchemaInline(consoleCaptureSchema(), consoleCapture, "console capture");
  const phase = manifest ? object(manifest.scenario, "capture manifest scenario").phase_id : undefined;
  const p03SegmentedCapture = phase === "P03";
  applySchemaInline(networkCaptureSchema(p03SegmentedCapture), networkCapture, "network capture");
  equal(consoleAllowlist.schema_revision, "gooseweb-console-allowlist/v6", "console allowlist revision");
  equal(networkAllowlist.schema_revision, "gooseweb-network-allowlist/v5", "network allowlist revision");
  const consoleBoundary = object(consoleAllowlist.capture_boundary, "console capture boundary");
  equal(consoleBoundary.source, "unfiltered agent-browser console output after document.readyState complete plus one second", "console capture source");
  equal(consoleBoundary.filtering, "none", "console filtering");
  equal(consoleBoundary.normalization, "none", "console normalization");
  equal(consoleBoundary.warnings_errors_exceptions_always_fail, true, "console failure policy");
  const capturedMessages = array(consoleCapture.messages, "console messages");
  if (capturedMessages.some((item) => ["warn", "error"].includes(string(object(item, "console message").level, "console level")))) fail("console capture contains warning/error/exception");
  const variants = array(consoleAllowlist.permitted_exact_variants, "console variants").map((item) => object(item, "console variant"));
  ensureUnique(variants.map((variant) => string(variant.variant_id, "console variant ID")), "console variant IDs");
  for (const variant of variants) if (array(variant.messages, "variant messages").some((item) => ["warn", "error"].includes(string(object(item, "variant message").level, "variant console level")))) fail("console allowlist variant contains warning/error");
  const benignLiterals: RecordJson[] = [
    { level: "debug", message: "[vite] connecting..." },
    { level: "debug", message: "[vite] connected." },
    { level: "info", message: "%cDownload the React DevTools for a better development experience: https://react.dev/link/react-devtools font-weight:bold" }
  ];
  const expectedPowerSet = Array.from({ length: 8 }, (_, mask) => benignLiterals.filter((_, index) => (mask & (1 << index)) !== 0)).map(multisetSignature).sort();
  const actualPowerSet = variants.map((variant) => multisetSignature(array(variant.messages, "variant messages"))).sort();
  equal(JSON.stringify(actualPowerSet), JSON.stringify(expectedPowerSet), "exact benign console literal power set");
  const matches = variants.filter((variant) => sameMultiset(capturedMessages, array(variant.messages, "variant messages")));
  if (matches.length !== 1) fail("console capture does not match exactly one finite benign variant");
  const boundary = object(networkAllowlist.capture_boundary, "network capture boundary");
  equal(boundary.source, "unfiltered agent-browser network requests including successes", "network capture source");
  equal(boundary.raw_capture_retained, true, "raw network retention");
  equal(boundary.query_values_retained, false, "network query-value redaction");
  equal(boundary.query_keys_retained, true, "network query-key retention");
  equal(boundary.failure_filtering_prohibited, true, "network failure filtering policy");
  equal(boundary.unclassified_filtering_prohibited, true, "unclassified network filtering policy");
  equal(boundary.ignored_status_min, 200, "ignored success status minimum");
  equal(boundary.ignored_status_max, 399, "ignored success status maximum");
  equal(JSON.stringify(boundary.ignored_same_origin_success_resource_types), JSON.stringify(["stylesheet", "font", "script", "module"]), "exact ignored static resource types");
  const ignoredTypes = new Set(array(boundary.ignored_same_origin_success_resource_types, "ignored resource types").map((item) => string(item, "resource type")));
  const rawHttp = array(networkCapture.raw_http, "raw HTTP capture");
  const evaluated = rawHttp.filter((item) => {
    const request = object(item, "HTTP request");
    const ignorableStatus = integer(request.status, "HTTP status") >= Number(boundary.ignored_status_min) && integer(request.status, "HTTP status") <= Number(boundary.ignored_status_max);
    return !(request.same_origin === true && ignorableStatus && ignoredTypes.has(string(request.resource_type, "resource type")));
  }).map((item) => {
    const request = object(item, "HTTP request");
    return { method: request.method!, path: request.path!, status: request.status!, resource_type: request.resource_type!, baseline_defect_id: request.baseline_defect_id! } as RecordJson;
  });
  for (const item of rawHttp) {
    const request = object(item, "HTTP request");
    const failed = integer(request.status, "HTTP status") >= 400;
    const baselineId = string(request.baseline_defect_id, "HTTP baseline ID");
    if (!failed && baselineId) fail("successful HTTP request cannot carry a failure baseline");
    if (failed) {
      if (!manifest) fail("failed HTTP request requires the validated manifest");
      const baseline = resolveManifestBaseline(manifest, baselineId, "HTTP failure");
      equal(baseline.scenario_id, object(manifest.scenario, "manifest scenario").stable_scenario_id, "HTTP baseline scenario");
    }
  }
  if (p03SegmentedCapture) {
    validateP03NetworkSegments(networkCapture, networkAllowlist, boundary, ignoredTypes);
  } else {
    const baseHttp: RecordJson[] = [
      { method: "GET", path: "/", status: 200, resource_type: "document", baseline_defect_id: "" },
      { method: "POST", path: "/api/dev-ticket", status: 200, resource_type: "api", baseline_defect_id: "" }
    ];
    const faviconFailure: RecordJson = { method: "GET", path: "/favicon.ico", status: 404, resource_type: "other", baseline_defect_id: "BASE-P01-FAVICON-NOT-FOUND" };
    const httpVariants = array(networkAllowlist.permitted_exact_evaluated_http_variants, "HTTP variants").map((item) => object(item, "HTTP variant"));
    ensureUnique(httpVariants.map((variant) => string(variant.variant_id, "HTTP variant ID")), "HTTP variant IDs");
    const expectedHttpVariants = [baseHttp, [...baseHttp, faviconFailure]].map(multisetSignature).sort();
    const actualHttpVariants = httpVariants.map((variant) => multisetSignature(array(variant.requests, "HTTP variant requests"))).sort();
    equal(JSON.stringify(actualHttpVariants), JSON.stringify(expectedHttpVariants), "exact evaluated HTTP variant set");
    const httpMatches = httpVariants.filter((variant) => sameMultiset(evaluated, array(variant.requests, "HTTP variant requests")));
    if (httpMatches.length !== 1) fail("evaluated HTTP activity does not match exactly one finite variant");
  }
  const websocket = object(networkCapture.websocket, "WebSocket capture");
  if (websocket.availability === "available") exactMultiset(array(websocket.events, "WebSocket events"), array(networkAllowlist.exact_websocket_events, "WebSocket allowlist"), "WebSocket events");
  else {
    equal(websocket.inference_prohibited, true, "unavailable WebSocket inference policy");
    requireText(websocket.reason, "unavailable WebSocket reason");
    const defectId = string(websocket.baseline_defect_id, "unavailable WebSocket baseline mapping");
    requireText(defectId, "unavailable WebSocket baseline mapping");
    if (!manifest) fail("unavailable WebSocket capture requires the validated manifest");
    const baseline = resolveManifestBaseline(manifest, defectId, "unavailable WebSocket");
    equal(baseline.scenario_id, object(manifest.scenario, "manifest scenario").stable_scenario_id, "WebSocket baseline scenario");
  }
  scanSecrets(consoleCapture, "console capture", false);
  scanSecrets(networkCapture, "network capture", false);
}
function validateP03NetworkSegments(
  networkCapture: RecordJson,
  networkAllowlist: RecordJson,
  boundary: RecordJson,
  ignoredTypes: ReadonlySet<string>
): void {
  const contract = object(networkAllowlist.p03_reconstruction_capture, "P03 network allowlist contract");
  equal(contract.schema_revision, "gooseweb-network-capture/v4", "P03 segmented network capture revision");
  equal(contract.raw_requests_retained, true, "P03 segmented raw network retention");
  const expectedSegments = array(contract.ordered_segments, "P03 expected network segments").map((entry) => object(entry, "P03 expected network segment"));
  const actualSegments = array(networkCapture.segments, "P03 captured network segments").map((entry) => object(entry, "P03 captured network segment"));
  equal(
    JSON.stringify(actualSegments.map((entry) => entry.segment_id)),
    JSON.stringify(expectedSegments.map((entry) => entry.segment_id)),
    "P03 exact ordered network segment identities"
  );
  const segmentIndex = new Map<string, number>();
  actualSegments.forEach((actual, index) => {
    const expected = expectedSegments[index]!;
    equal(actual.trigger, expected.trigger, `P03 network segment ${index} trigger`);
    equal(actual.context_generation, expected.context_generation, `P03 network segment ${index} context generation`);
    equal(actual.complete, true, `P03 network segment ${index} completeness`);
    equal(actual.capture_started_before_trigger, true, `P03 network segment ${index} start boundary`);
    equal(actual.capture_ended_after_observable_state, true, `P03 network segment ${index} end boundary`);
    segmentIndex.set(string(actual.segment_id, "P03 segment ID"), index);
  });
  const rawHttp = array(networkCapture.raw_http, "P03 raw HTTP capture").map((entry) => object(entry, "P03 raw HTTP request"));
  let previousSegmentIndex = -1;
  for (const request of rawHttp) {
    const id = string(request.segment_id, "P03 request segment ID");
    const index = segmentIndex.get(id);
    if (index === undefined) fail("P03 raw request references an unknown reconstruction segment");
    if (index < previousSegmentIndex) fail("P03 raw request segment attribution is not monotonic");
    previousSegmentIndex = index;
  }
  const required = array(contract.required_evaluated_requests_per_segment, "P03 required requests");
  const optionalContract = object(contract.optional_favicon_request_per_segment, "P03 optional favicon contract");
  equal(optionalContract.maximum_count, 1, "P03 optional favicon maximum");
  const optionalFavicon = object(optionalContract.request, "P03 optional favicon request");
  for (const segment of actualSegments) {
    const id = string(segment.segment_id, "P03 captured segment ID");
    const segmentRaw = rawHttp.filter((request) => request.segment_id === id);
    equal(segment.raw_request_count, segmentRaw.length, `P03 ${id} derived raw request count`);
    const segmentEvaluated = segmentRaw.filter((request) => {
      const status = integer(request.status, `P03 ${id} HTTP status`);
      const ignorableStatus = status >= Number(boundary.ignored_status_min) && status <= Number(boundary.ignored_status_max);
      return !(request.same_origin === true && ignorableStatus && ignoredTypes.has(string(request.resource_type, `P03 ${id} resource type`)));
    }).map(p03EvaluatedRequest);
    const withoutFavicon = sameMultiset(segmentEvaluated, required);
    const withFavicon = sameMultiset(segmentEvaluated, [...required, optionalFavicon]);
    if (!withoutFavicon && !withFavicon) fail(`P03 ${id} evaluated HTTP activity does not match the exact reconstruction contract`);
  }
}

function p03EvaluatedRequest(request: RecordJson): RecordJson {
  return {
    method: request.method!,
    path: request.path!,
    query_keys: request.query_keys!,
    status: request.status!,
    resource_type: request.resource_type!,
    same_origin: request.same_origin!,
    baseline_defect_id: request.baseline_defect_id!
  };
}
