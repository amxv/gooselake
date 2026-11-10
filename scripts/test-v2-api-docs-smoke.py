#!/usr/bin/env python3
"""Offline HTTP acceptance for documented v2 operator behavior.

Runs an isolated loopback gg-runtime-server with an inert, never-invoked ACP
provider. No real provider session/device, production database or host process
is touched. All server and SQLite state live in a disposable temporary root.
"""
from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(
    os.environ.get("GG_RUNTIME_DOCS_SMOKE_BIN", ROOT / "target/debug/gg-runtime-server")
).expanduser().resolve()


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)


def reserve_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


class OfflineRuntime:
    def __init__(self, base: Path) -> None:
        self.base = base
        self.port = reserve_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.token = secrets.token_hex(24)
        self.proc: subprocess.Popen[bytes] | None = None
        self.log = (base / "runtime.log").open("ab", buffering=0)
        self.marker = base / "provider-was-launched"
        stub = base / "do-not-start-acp"
        stub.write_text(f'#!/bin/sh\nprintf started > "{self.marker}"\nexit 92\n')
        stub.chmod(0o700)
        self.config = base / "runtime.toml"
        self.config.write_text(
            f'[server]\nbind_address = "127.0.0.1:{self.port}"\n'
            f'public_base_url = "{self.url}"\n'
            f'[auth]\nmode = "static_bearer"\ntoken = "{self.token}"\n'
            f'[data]\nroot_dir = "{base / "state"}"\n'
            f'[providers.codex]\nenabled = false\n'
            f'[providers.claude]\nenabled = false\n'
            f'[providers.acp]\nenabled = true\ncommand = "{stub}"\n'
            f'transport = "stdio"\n'
            f'[processes]\nenabled = false\n'
            f'[worktrees]\nenabled = false\n'
        )

    def start(self) -> None:
        require(self.proc is None, "cannot start duplicate disposable runtime")
        self.proc = subprocess.Popen(
            [str(BINARY), "--config", str(self.config)],
            stdout=self.log, stderr=subprocess.STDOUT,
            cwd=self.base,
        )
        for _ in range(60):
            if self.proc.poll() is not None:
                raise RuntimeError(f"isolated runtime exited {self.proc.returncode}")
            try:
                with urlopen(f"{self.url}/health", timeout=0.5) as response:
                    if response.status == 200:
                        body = json.load(response)
                        require(body["status"] == "ok" and body["providers"] == 1,
                                "unexpected loopback health response")
                        require(self.proc.poll() is None,
                                "disposable runtime exited while another service answered")
                        return
            except (URLError, TimeoutError):
                time.sleep(0.1)
        raise RuntimeError("disposable runtime was not ready within bounded retries")

    def stop(self) -> None:
        if self.proc is None:
            return
        self.proc.terminate()  # Only the subprocess created by this fixture.
        try:
            self.proc.wait(timeout=7)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=3)
        self.proc = None

    def close(self) -> None:
        self.stop()
        self.log.close()

    def call(
        self, method: str, route: str, *,
        data: dict | None = None, auth: bool = True,
        headers: dict[str, str] | None = None,
        status: int = 200,
    ) -> tuple[object, http.client.HTTPMessage]:
        request_headers = dict(headers or {})
        if auth:
            request_headers["Authorization"] = f"Bearer {self.token}"
        encoded = None if data is None else json.dumps(data, sort_keys=True).encode("utf-8")
        if encoded is not None:
            request_headers["Content-Type"] = "application/json"
        request = Request(
            self.url + route, data=encoded, headers=request_headers, method=method,
        )
        try:
            result = urlopen(request, timeout=5)
        except HTTPError as error:
            result = error
        with result:
            code = result.status
            body = result.read().decode("utf-8")
            received_headers = result.headers
        require(code == status, f"{method} {route}: expected HTTP {status}, got {code}: {body[:800]}")
        try:
            content: object = json.loads(body)
        except (ValueError, TypeError):
            content = body
        return content, received_headers

    def duplicate_operator_auth_is_rejected(self) -> None:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.putrequest("GET", "/v2/workspaces")
            for _ in range(2):
                connection.putheader("Authorization", f"Bearer {self.token}")
            connection.endheaders()
            result = connection.getresponse()
            response_status = result.status
            result.read()
            require(response_status == 401,
                    f"duplicate operator Authorization unexpectedly returned {response_status}")
        finally:
            connection.close()


def run(runtime: OfflineRuntime) -> None:
    runtime.start()
    caps, _ = runtime.call("GET", "/v2/capabilities")
    require(isinstance(caps, dict) and "api_version" in caps and "providers" in caps,
            "runtime capability payload missing canonical keys")
    unauthorized, _ = runtime.call("GET", "/v2/workspaces", auth=False, status=401)
    require(isinstance(unauthorized, dict) and unauthorized["error"]["code"] == "unauthorized",
            "operator unauthorized error envelope drift")
    runtime.duplicate_operator_auth_is_rejected()
    runtime.call("GET", "/v2/mcp/capabilities", status=401)  # Operator != scoped MCP.

    project_a, project_b = runtime.base / "project-a", runtime.base / "project-b"
    project_a.mkdir()
    project_b.mkdir()
    body_a = {"canonical_root": str(project_a), "display_name": "Offline A"}
    body_b = {"canonical_root": str(project_b), "display_name": "Offline B"}

    first, first_headers = runtime.call(
        "POST", "/v2/workspaces", data=body_a,
        headers={"Idempotency-Key": "docs-smoke-register-a"},
    )
    require(isinstance(first, dict), "registration result is not JSON")
    workspace_a = first["workspace"]["workspace_id"]
    operation_a = first["operation_id"]
    require(first_headers.get("X-Operation-ID") == operation_a and
            bool(first_headers.get("X-Request-ID")), "operator response headers drift")
    require(first["workspace"]["lead_agent_id"] is None and
            first["workspace"]["revision"] == 0, "new workspace should be leadless")

    replay, _ = runtime.call(
        "POST", "/v2/workspaces", data=body_a,
        headers={"Idempotency-Key": "docs-smoke-register-a"},
    )
    require(replay == first, "same-key registration did not replay exact receipt")
    conflict, _ = runtime.call(
        "POST", "/v2/workspaces",
        data={"canonical_root": str(project_a), "display_name": "Changed"},
        headers={"Idempotency-Key": "docs-smoke-register-a"}, status=409,
    )
    require(conflict["error"]["code"] == "conflict", "same-key changed-input conflict drift")
    runtime.call("POST", "/v2/workspaces", status=422,
                 data={"canonical_root": str(project_a), "lead_agent_id": "forged"})

    second, _ = runtime.call(
        "POST", "/v2/workspaces", data=body_b,
        headers={"Idempotency-Key": "docs-smoke-register-b"},
    )
    require(second["workspace"]["workspace_id"] != workspace_a, "distinct roots merged")
    page1, page1_headers = runtime.call("GET", "/v2/workspaces?limit=1")
    require(isinstance(page1, list) and len(page1) == 1, "list must be bounded JSON array")
    cursor = page1_headers.get("X-Next-Cursor")
    require(bool(cursor), "missing first-page keyset response header")
    page2, page2_headers = runtime.call(
        "GET", "/v2/workspaces?" + urlencode({"cursor": cursor, "limit": 1}),
    )
    require(isinstance(page2, list) and len(page2) == 1 and
            page2[0]["workspace_id"] != page1[0]["workspace_id"] and
            not page2_headers.get("X-Next-Cursor"), "second-page cursor/ordering drift")
    runtime.call("GET", "/v2/workspaces?cursor=not-a-valid-cursor", status=400)

    item, item_headers = runtime.call("GET", f"/v2/workspaces/{workspace_a}")
    require(item["workspace_id"] == workspace_a and item_headers.get("ETag") == '"0"',
            "strong workspace revision ETag drift")
    roster, _ = runtime.call("GET", f"/v2/workspaces/{workspace_a}/agents")
    require(roster == [], "new workspace must have no independent legacy team members")
    stale, _ = runtime.call(
        "POST", f"/v2/workspaces/{workspace_a}/lead",
        data={"expected_revision": 0, "lead_agent_id": None},
        headers={"If-Match": '"999"'}, status=412,
    )
    require(stale["error"]["code"] == "revision_precondition_failed",
            "stale workspace If-Match error drift")

    history, _ = runtime.call("GET", "/v2/events?limit=1")
    require(isinstance(history, list), "event history must return JSON array")
    runtime.call("GET", "/v2/events?limit=0", status=400)
    runtime.call("GET", f"/v2/workspaces/{workspace_a}/events/stream?after_seq=-1",
                 status=400)
    require(not runtime.marker.exists(), "API smoke accidentally spawned ACP provider")

    runtime.stop()
    runtime.start()
    persisted, _ = runtime.call("GET", f"/v2/workspaces/{workspace_a}")
    require(persisted["workspace_id"] == workspace_a, "workspace ID did not survive restart")
    after_restart, _ = runtime.call(
        "POST", "/v2/workspaces", data=body_a,
        headers={"Idempotency-Key": "docs-smoke-register-a"},
    )
    require(after_restart == first, "idempotent operation receipt lost after restart")
    require(not runtime.marker.exists(), "API smoke accidentally spawned ACP provider")


def main() -> int:
    require(BINARY.is_file() and os.access(BINARY, os.X_OK),
            f"runtime smoke binary missing or non-executable: {BINARY}")
    with tempfile.TemporaryDirectory(prefix="gg-v2-api-docs-smoke.") as directory:
        runtime = OfflineRuntime(Path(directory))
        try:
            subprocess.run(
                [str(BINARY), "--check-config", "--config", str(runtime.config)],
                cwd=runtime.base, check=True, capture_output=True, timeout=10,
            )
            run(runtime)
        except Exception:
            runtime.log.flush()
            runtime.close()
            print("Disposable runtime log (last 40 lines):", file=sys.stderr)
            print("\n".join((runtime.base / "runtime.log").read_text().splitlines()[-40:]),
                  file=sys.stderr)
            raise
        finally:
            runtime.close()
    print("Offline v2 API docs smoke: PASS (auth, workspace, keyset, ETag, event, restart; no provider launched)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
