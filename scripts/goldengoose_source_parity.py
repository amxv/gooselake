#!/usr/bin/env python3
"""Read-only static manifest drift check against a checked-out Golden Goose.

No clones/network, provider sessions, database mutations, or live proof. A
pinned Golden Goose source fingerprint makes manual harness/context review
mandatory whenever its implementation changes; these complex semantics
cannot be certified equivalent from catalog matching alone.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


HERE = Path(__file__).resolve().parent
GOOSELAKE_ROOT = HERE.parent
BASELINE_PATH = HERE / "goldengoose-source-baseline.json"
GG_CODEX = "src/config/codex-model-policy.json"
GL_CODEX = "crates/runtime-core/assets/codex-model-policy.json"
GG_MODELS = "src-tauri/src/agent_runtime/runtime_model_catalog.rs"
GL_MODELS = "crates/runtime-core/src/model_policy.rs"
GG_MCP = "src-tauri/src/agent_runtime/mcp_tool_gateway/mod.rs"
GL_MCP = "crates/runtime-server/src/http/mcp.rs"
GG_PROMPT = "src-tauri/prompts/goldengoose-harness-instructions.md"
GL_PROMPT = "crates/runtime-core/assets/gooselake-harness-v1.md"
GG_CONTEXT = "src-tauri/src/types/agents/models/core.rs"
GG_CONTEXT_OWNER = "src-tauri/src/agent_runtime/control_plane/model.rs"
GL_CONTEXT = "crates/runtime-core/src/provider_contract.rs"
GG_TEAM = "src-tauri/tool-contracts/gg_team.json"
GG_PROCESS = "src-tauri/tool-contracts/gg_process.json"
GL_SEMANTIC = "crates/runtime-core/src/provider_contract.rs"
CLAUDE_KINDS = frozenset({"OPUS", "FABLE", "HAIKU", "SONNET"})
MODEL_FAMILIES = {"gg_team", "gg_message", "gg_process"}
CONTEXT_FIELDS = {
    "model_context_window",
    "last_total_tokens",
    "remaining_percentage",
    "provider_session_ref",
}
PINNED_REFERENCE_FILES = frozenset({
    GG_PROMPT,
    "src-tauri/src/agent_runtime/harness_instructions.rs",
    GG_CONTEXT_OWNER,
    GG_MCP,
    GG_MODELS,
    GG_CONTEXT,
})


def _text(root: Path, relative: str) -> str:
    path = root / relative
    if not path.is_file():
        raise ValueError(f"required source file not available: {relative}")
    return path.read_text(encoding="utf-8")


def _json(root: Path, relative: str) -> object:
    return json.loads(_text(root, relative))


def _claude_models(source: str, *, golden_goose: bool) -> dict[str, str]:
    expression = (
        r'pub const CLAUDE_(OPUS|FABLE|HAIKU|SONNET)_\d+_\d+_MODEL:\s*&str\s*=\s*"([^"]+)"'
        if golden_goose
        else r'pub const CLAUDE_(OPUS|FABLE|HAIKU|SONNET)_MODEL:\s*&str\s*=\s*"([^"]+)"'
    )
    matches = re.findall(expression, source)
    found = dict(matches)
    if len(matches) != len(found) or set(found) != CLAUDE_KINDS:
        raise ValueError("Claude model declarations incomplete or ambiguous")
    return found


def _efforts(source: str, name: str) -> tuple[str, ...]:
    expression = r"\b" + re.escape(name) + r':\s*&\[&str\]\s*=\s*&\[(.*?)\];'
    match = re.search(expression, source, re.DOTALL)
    if not match:
        raise ValueError(f"missing Claude advanced effort constant: {name}")
    efforts = tuple(re.findall(r'"([^"]+)"', match.group(1)))
    if not efforts or len(efforts) != len(set(efforts)):
        raise ValueError(f"invalid Claude efforts: {name}")
    return efforts


def _scoped_tools(source: str, *, golden_goose: bool) -> set[str]:
    expression = (
        r'let is_supported_tool\s*=\s*matches!\(\s*tool_name,\s*(.*?)\s*\);'
        if golden_goose
        else r'if\s+!matches!\(\s*request\.tool_name\.as_str\(\),\s*(.*?)\s*\)'
    )
    match = re.search(expression, source, re.DOTALL)
    if not match:
        raise ValueError("scoped MCP three-tool allowlist not found")
    names = re.findall(r'"(gg_[a-z_]+)"', match.group(1))
    if len(names) != len(set(names)):
        raise ValueError("duplicate scoped MCP tool identifiers")
    return set(names)


def _sections(markdown: str) -> tuple[str, ...]:
    return tuple(re.findall(r"^# ([A-Za-z]+)\s*$", markdown, re.MULTILINE))


def _gg_modes(root: Path, relative: str, which: str) -> set[str]:
    value = _json(root, relative)
    if not isinstance(value, dict):
        raise ValueError(f"invalid {which} tool contract")
    if which == "gg_team":
        options = value["inputSchema"]["oneOf"]
        modes = [entry["properties"]["mode"]["const"] for entry in options]
    else:
        options = value["codexInputSchema"]["oneOf"]
        enums = [entry["properties"]["mode"]["enum"] for entry in options]
        if any(len(values) != 1 for values in enums):
            raise ValueError(f"{which} mode variants must each have one explicit enum")
        modes = [values[0] for values in enums]
    if not modes or not all(isinstance(mode, str) and mode for mode in modes):
        raise ValueError(f"{which} contract has empty or invalid semantic modes")
    if len(modes) != len(set(modes)):
        raise ValueError(f"{which} contract contains duplicated semantic modes")
    return set(modes)


def _gl_modes(source: str, which: str) -> set[str]:
    expression = (
        r'"name"\s*:\s*"' + re.escape(which)
        + r'"\s*,\s*"semantic_modes"\s*:\s*\[(.*?)\]'
    )
    match = re.search(expression, source, re.DOTALL)
    if not match:
        raise ValueError(f"GooseLake semantic {which} mode contract missing")
    modes = re.findall(r'"([^"]+)"', match.group(1))
    if not modes or len(modes) != len(set(modes)):
        raise ValueError(f"GooseLake {which} mode list empty or duplicated")
    return set(modes)


def _git_commit(root: Path) -> str | None:
    if not (root / ".git").exists():
        return None
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=10, check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def inspect(
    golden_goose_root: Path,
    gooselake_root: Path = GOOSELAKE_ROOT,
    baseline: dict[str, object] | None = None,
) -> dict[str, object]:
    if baseline is None:
        baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    reference_commit = baseline.get("reference_commit")
    fingerprints = baseline.get("source_fingerprints")
    if not isinstance(reference_commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}", reference_commit
    ):
        raise ValueError("source baseline must pin a full Golden Goose commit SHA")
    if not isinstance(fingerprints, dict) or set(fingerprints) != PINNED_REFERENCE_FILES:
        raise ValueError("source baseline must pin all six critical Golden Goose files")
    divergences = baseline.get("approved_divergences")
    if not isinstance(divergences, list) or not all(
        isinstance(value, str) and value for value in divergences
    ):
        raise ValueError("baseline must enumerate intentional divergences")
    checks: list[dict[str, object]] = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"check": name, "passed": bool(ok), "detail": detail})

    for relative, expected_hash in sorted(fingerprints.items()):
        if not isinstance(expected_hash, str) or not re.fullmatch(
            r"[0-9a-f]{64}", expected_hash
        ):
            raise ValueError("invalid fingerprint record")
        actual = hashlib.sha256(
            _text(golden_goose_root, relative).encode("utf-8")
        ).hexdigest()
        check(
            f"reference_fingerprint:{relative}",
            actual == expected_hash,
            "pinned source unchanged" if actual == expected_hash
            else "source changed; manually review harness/context/tool implications",
        )

    actual_commit = _git_commit(golden_goose_root)
    check(
        "reference_revision",
        actual_commit == reference_commit,
        "pinned Golden Goose revision" if actual_commit == reference_commit
        else "different or unverifiable revision; update baseline after manual review",
    )

    codex_equal = _json(golden_goose_root, GG_CODEX) == _json(gooselake_root, GL_CODEX)
    check("codex_catalog_and_defaults", codex_equal, "exact JSON policy equality")

    gg_models = _text(golden_goose_root, GG_MODELS)
    gl_models = _text(gooselake_root, GL_MODELS)
    claude_equal = _claude_models(gg_models, golden_goose=True) == _claude_models(
        gl_models, golden_goose=False
    )
    check("claude_catalog", claude_equal, "Opus/Fable/Haiku/Sonnet model ID parity")

    efforts_equal = _efforts(
        gg_models, "CLAUDE_ADVANCED_THINKING_EFFORTS"
    ) == _efforts(gl_models, "CLAUDE_ADVANCED_EFFORTS")
    check("claude_advanced_efforts", efforts_equal, "ordered effort tokens")

    gg_tools = _scoped_tools(
        _text(golden_goose_root, GG_MCP), golden_goose=True
    )
    gl_tools = _scoped_tools(
        _text(gooselake_root, GL_MCP), golden_goose=False
    )
    check(
        "scoped_tool_allowlist",
        gg_tools == gl_tools == MODEL_FAMILIES,
        "exactly gg_team, gg_message, gg_process; legacy operator tools excluded",
    )

    semantic = _text(gooselake_root, GL_SEMANTIC)
    for name, relative in [
        ("gg_team", GG_TEAM),
        ("gg_process", GG_PROCESS),
    ]:
        upstream_modes = _gg_modes(golden_goose_root, relative, name)
        local_modes = _gl_modes(semantic, name)
        check(
            f"{name}_semantic_modes",
            upstream_modes == local_modes and bool(upstream_modes),
            "exact semantic mode set; argument/result schemas require independent review",
        )

    golden_prompt = _text(golden_goose_root, GG_PROMPT)
    local_prompt = _text(gooselake_root, GL_PROMPT)
    expected_sections = ("Shared", "Codex", "Claude")
    sections_valid = (
        _sections(golden_prompt) == expected_sections
        and _sections(local_prompt) == expected_sections
        and all(
            name in golden_prompt and name in local_prompt
            for name in sorted(MODEL_FAMILIES)
        )
    )
    check(
        "harness_structure_and_tool_mentions", sections_valid,
        "shared/Codex/Claude order and three semantic tool references; text differs intentionally",
    )

    # Golden Goose keeps the exposed usage measurements in shared UI types,
    # while the provider-session binding lives in its authority record.
    upstream_context = (
        _text(golden_goose_root, GG_CONTEXT)
        + "\n"
        + _text(golden_goose_root, GG_CONTEXT_OWNER)
    )
    local_context = _text(gooselake_root, GL_CONTEXT)
    context_fields_valid = all(
        token in upstream_context and token in local_context
        for token in CONTEXT_FIELDS
    )
    check(
        "context_observation_field_presence", context_fields_valid,
        "shared evidence fields only; recovery/ownership semantics NOT statically certified",
    )

    return {
        "kind": "gooselake_goldengoose_static_parity",
        "reference_commit": actual_commit,
        "expected_reference_commit": reference_commit,
        "checked_only_read_only_source": True,
        "provider_live_accepted": False,
        "context_recovery_semantics_accepted": False,
        "final_cutover_authorized": False,
        "approved_divergences": divergences,
        "checks": checks,
        "passed": all(row["passed"] for row in checks),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--goldengoose-root", required=True, type=Path,
        help="path to a locally checked-out Golden Goose source tree; never cloned by this tool",
    )
    args = parser.parse_args()
    try:
        report = inspect(args.goldengoose_root)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"Golden Goose source parity audit refused: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
