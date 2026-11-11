#!/usr/bin/env python3
"""Offline synthetic drift/fail-closed tests for the optional GG source audit."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("goldengoose_source_parity.py")
ROOT = SCRIPT.parent.parent
PINNED_SHA = "a" * 40
spec = importlib.util.spec_from_file_location("goldengoose_source_parity", SCRIPT)
assert spec is not None and spec.loader is not None
parity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parity)


class SourceParityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="gg-source-parity-")
        self.addCleanup(self.temp.cleanup)
        self.reference = Path(self.temp.name) / "goldengoose"
        self.local = Path(self.temp.name) / "gooselake"
        local_paths = {
            parity.GL_CODEX, parity.GL_MODELS, parity.GL_MCP,
            parity.GL_PROMPT, parity.GL_CONTEXT, parity.GL_SEMANTIC,
        }
        for relative in local_paths:
            self.write(self.local, relative, (ROOT / relative).read_text())

        self.write(
            self.reference, parity.GG_CODEX,
            (self.local / parity.GL_CODEX).read_text(),
        )
        model_source = (self.local / parity.GL_MODELS).read_text()
        models = parity._claude_models(model_source, golden_goose=False)
        efforts = parity._efforts(model_source, "CLAUDE_ADVANCED_EFFORTS")
        model_lines = [
            f'pub const CLAUDE_{name}_5_5_MODEL: &str = "{model}";'
            for name, model in sorted(models.items())
        ]
        model_lines.append(
            "pub const CLAUDE_ADVANCED_THINKING_EFFORTS: &[&str] = &["
            + ", ".join(json.dumps(effort) for effort in efforts) + "];"
        )
        self.write(self.reference, parity.GG_MODELS, "\n".join(model_lines))
        self.write(
            self.reference, parity.GG_MCP,
            'let is_supported_tool = matches!(tool_name, '
            '"gg_team" | "gg_message" | "gg_process");',
        )
        self.write(
            self.reference, parity.GG_PROMPT,
            (self.local / parity.GL_PROMPT).read_text(),
        )
        self.write(
            self.reference, "src-tauri/src/agent_runtime/harness_instructions.rs",
            "pub fn harness_revision() -> u32 { 1 }\n",
        )
        self.write(
            self.reference, parity.GG_CONTEXT,
            "\n".join(sorted(parity.CONTEXT_FIELDS)),
        )
        self.write(
            self.reference, parity.GG_CONTEXT_OWNER,
            "provider_session_ref",
        )
        self.write(
            self.reference, parity.GG_TEAM,
            json.dumps({
                "inputSchema": {"oneOf": [
                    {"properties": {"mode": {"const": mode}}}
                    for mode in sorted(parity._gl_modes(
                        (self.local / parity.GL_SEMANTIC).read_text(), "gg_team"
                    ))
                ]}
            }),
        )
        self.write(
            self.reference, parity.GG_PROCESS,
            json.dumps({
                "codexInputSchema": {"oneOf": [
                    {"properties": {"mode": {"enum": [mode]}}}
                    for mode in sorted(parity._gl_modes(
                        (self.local / parity.GL_SEMANTIC).read_text(), "gg_process"
                    ))
                ]}
            }),
        )
        self.baseline = {
            "reference_commit": PINNED_SHA,
            "source_fingerprints": {
                relative: hashlib.sha256(
                    (self.reference / relative).read_bytes()
                ).hexdigest()
                for relative in sorted(parity.PINNED_REFERENCE_FILES)
            },
            "approved_divergences": ["Synthetic test; no live equivalence claim"],
        }

    @staticmethod
    def write(root: Path, relative: str, value: str) -> None:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(value)

    def inspect(self, commit: str = PINNED_SHA) -> dict[str, object]:
        with patch.object(parity, "_git_commit", return_value=commit), patch.object(
            parity, "_reference_checkout_clean", return_value=True
        ):
            return parity.inspect(self.reference, self.local, self.baseline)

    def test_matching_synthetic_source_is_static_only_and_read_only(self) -> None:
        paths = sorted(
            path for root in [self.reference, self.local]
            for path in root.rglob("*") if path.is_file()
        )
        before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        report = self.inspect()
        self.assertTrue(report["passed"], report["checks"])
        self.assertEqual(len(report["checks"]), 16)
        self.assertTrue(report["checked_only_read_only_source"])
        self.assertFalse(report["provider_live_accepted"])
        self.assertFalse(report["context_recovery_semantics_accepted"])
        self.assertFalse(report["final_cutover_authorized"])
        self.assertEqual(
            before,
            {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
        )
        self.assertEqual(paths, sorted(
            path for root in [self.reference, self.local]
            for path in root.rglob("*") if path.is_file()
        ))

    def test_upstream_source_changed_requires_manual_review(self) -> None:
        path = self.reference / parity.GG_PROMPT
        path.write_text(path.read_text() + "\n# Unreviewed\n")
        report = self.inspect()
        self.assertFalse(report["passed"])
        self.assertFalse(next(row["passed"] for row in report["checks"]
                              if row["check"] == f"reference_fingerprint:{parity.GG_PROMPT}"))
        self.assertFalse(next(row["passed"] for row in report["checks"]
                              if row["check"] == "harness_structure_and_tool_mentions"))

    def test_new_upstream_commit_cannot_pass_with_stale_baseline(self) -> None:
        report = self.inspect(commit="b" * 40)
        self.assertFalse(report["passed"])
        self.assertFalse(next(row["passed"] for row in report["checks"]
                              if row["check"] == "reference_revision"))

    def test_dirty_upstream_contract_fails_even_when_pinned_commit_matches(self) -> None:
        # A non-pinned mode-argument description could change without changing
        # the semantic mode names. Such an uncommitted edit must not pass.
        with patch.object(parity, "_git_commit", return_value=PINNED_SHA), patch.object(
            parity, "_reference_checkout_clean", return_value=False
        ):
            report = parity.inspect(self.reference, self.local, self.baseline)
        self.assertFalse(report["passed"])
        self.assertTrue(next(row["passed"] for row in report["checks"]
                             if row["check"] == "reference_revision"))
        self.assertFalse(next(row["passed"] for row in report["checks"]
                              if row["check"] == "reference_worktree_clean"))

    def test_actual_git_checkout_staged_and_unstaged_changes_fail_closed(self) -> None:
        subprocess.run(["git", "init", "-q", str(self.reference)],
                       check=True, capture_output=True, text=True, timeout=10)
        subprocess.run(["git", "-C", str(self.reference), "add", "-A"],
                       check=True, capture_output=True, text=True, timeout=10)
        subprocess.run([
            "git", "-C", str(self.reference), "-c", "user.name=Fixture",
            "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture",
        ], check=True, capture_output=True, text=True, timeout=10)
        self.assertTrue(parity._reference_checkout_clean(self.reference))
        path = self.reference / parity.GG_PROCESS
        path.write_text(path.read_text() + "\n")
        self.assertFalse(parity._reference_checkout_clean(self.reference))
        subprocess.run(["git", "-C", str(self.reference), "add", parity.GG_PROCESS],
                       check=True, capture_output=True, text=True, timeout=10)
        self.assertFalse(parity._reference_checkout_clean(self.reference))

    def test_local_model_policy_drift_is_detected(self) -> None:
        local_catalog = self.local / parity.GL_CODEX
        policy = json.loads(local_catalog.read_text())
        policy["unreviewed_local_change"] = True
        local_catalog.write_text(json.dumps(policy))
        report = self.inspect()
        self.assertFalse(report["passed"])
        self.assertFalse(next(row["passed"] for row in report["checks"]
                              if row["check"] == "codex_catalog_and_defaults"))

    def test_reference_tool_mode_drift_is_detected(self) -> None:
        tool = self.reference / parity.GG_TEAM
        contract = json.loads(tool.read_text())
        contract["inputSchema"]["oneOf"].append({
            "properties": {"mode": {"const": "unreviewed_mode"}}
        })
        tool.write_text(json.dumps(contract))
        report = self.inspect()
        self.assertFalse(report["passed"])
        self.assertFalse(next(row["passed"] for row in report["checks"]
                              if row["check"] == "gg_team_semantic_modes"))

    def test_duplicate_or_ambiguous_upstream_modes_fail_closed(self) -> None:
        tool = self.reference / parity.GG_PROCESS
        contract = json.loads(tool.read_text())
        contract["codexInputSchema"]["oneOf"][0]["properties"]["mode"]["enum"].append(
            "unreviewed_mode"
        )
        tool.write_text(json.dumps(contract))
        with self.assertRaisesRegex(ValueError, "one explicit enum"):
            self.inspect()

        team = self.reference / parity.GG_TEAM
        contract = json.loads(team.read_text())
        contract["inputSchema"]["oneOf"].append(contract["inputSchema"]["oneOf"][0])
        team.write_text(json.dumps(contract))
        with self.assertRaisesRegex(ValueError, "duplicated"):
            self.inspect()

    def test_missing_pins_or_unverifiable_commit_fail_closed(self) -> None:
        self.baseline["source_fingerprints"].pop(parity.GG_PROMPT)
        with self.assertRaisesRegex(ValueError, "all six critical"):
            self.inspect()
        self.baseline["source_fingerprints"][parity.GG_PROMPT] = "not-a-sha256"
        with self.assertRaisesRegex(ValueError, "invalid fingerprint"):
            self.inspect()
        self.baseline["source_fingerprints"][parity.GG_PROMPT] = hashlib.sha256(
            (self.reference / parity.GG_PROMPT).read_bytes()
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "full Golden Goose commit"):
            self.baseline["reference_commit"] = ""
            self.inspect()

    def test_missing_reference_cli_returns_nonzero_without_cloning(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--goldengoose-root",
             str(Path(self.temp.name) / "missing")],
            capture_output=True, text=True, timeout=10, check=False,
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("audit refused", result.stderr)
        self.assertFalse((Path(self.temp.name) / "missing").exists())


if __name__ == "__main__":
    unittest.main()
