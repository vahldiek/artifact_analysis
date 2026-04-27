"""Tests for src.orchestrator — Python pipeline orchestrator."""

from __future__ import annotations

from unittest.mock import patch

from src.config import PipelineConfig
from src.orchestrator import _should_skip, _stage_argv, run_pipeline
from src.stages import STAGE_MAP, STAGES


class TestStageArgv:
    def test_statistics_has_conf_regex_and_output(self):
        cfg = PipelineConfig(output_dir="/out", conf_regex="sosp2024")
        stage = STAGE_MAP["statistics"]
        args = _stage_argv(stage, cfg)
        assert "--conf_regex" in args
        assert "sosp2024" in args
        assert "--output_dir" in args
        assert "/out" in args

    def test_author_stats_has_dblp_and_data_and_output(self):
        cfg = PipelineConfig(output_dir="/out", dblp_file="/dblp.xml.gz")
        stage = STAGE_MAP["author_stats"]
        args = _stage_argv(stage, cfg)
        assert "--dblp_file" in args
        assert "--data_dir" in args
        assert "--output_dir" in args

    def test_all_stages_have_args(self):
        cfg = PipelineConfig()
        for stage in STAGES:
            args = _stage_argv(stage, cfg)
            assert isinstance(args, list)


class TestShouldSkip:
    def test_dblp_extract_skipped_when_missing(self, tmp_path):
        cfg = PipelineConfig(dblp_file=tmp_path / "missing.xml.gz")
        assert _should_skip(STAGE_MAP["dblp_extract"], cfg) is True

    def test_dblp_extract_not_skipped_when_present(self, tmp_path):
        f = tmp_path / "dblp.xml.gz"
        f.touch()
        cfg = PipelineConfig(dblp_file=f)
        assert _should_skip(STAGE_MAP["dblp_extract"], cfg) is False

    def test_statistics_never_skipped(self, tmp_path):
        cfg = PipelineConfig(dblp_file=tmp_path / "missing.xml.gz")
        assert _should_skip(STAGE_MAP["statistics"], cfg) is False


class TestRunPipeline:
    @patch("src.orchestrator._check_dblp")
    @patch("src.orchestrator._detect_github_token")
    @patch("src.orchestrator._call_main")
    def test_all_stages_succeed(self, mock_call, mock_token, mock_dblp, tmp_path):
        """All stages return cleanly → pipeline succeeds."""
        mock_call.return_value = None
        cfg = PipelineConfig(output_dir=tmp_path / "out", log_dir=tmp_path / "logs")
        dblp = tmp_path / "dblp.xml.gz"
        dblp.touch()
        cfg.dblp_file = dblp

        result = run_pipeline(cfg)
        assert result is True
        assert mock_call.call_count == len(STAGES)

    @patch("src.orchestrator._check_dblp")
    @patch("src.orchestrator._detect_github_token")
    @patch("src.orchestrator._call_main")
    def test_required_stage_failure_aborts(self, mock_call, mock_token, mock_dblp, tmp_path):
        """A required stage raising returns False."""
        mock_call.side_effect = RuntimeError("boom")
        cfg = PipelineConfig(output_dir=tmp_path / "out", log_dir=tmp_path / "logs")
        dblp = tmp_path / "dblp.xml.gz"
        dblp.touch()
        cfg.dblp_file = dblp

        result = run_pipeline(cfg)
        assert result is False

    @patch("src.orchestrator._check_dblp")
    @patch("src.orchestrator._detect_github_token")
    @patch("src.orchestrator._call_main")
    def test_optional_stage_failure_continues(self, mock_call, mock_token, mock_dblp, tmp_path):
        """Optional stages raising should not abort the pipeline."""

        def side_effect(module_name, argv):
            stage = next((s for s in STAGES if s.module == module_name), None)
            if stage and stage.optional:
                msg = "optional fail"
                raise RuntimeError(msg)
            return

        mock_call.side_effect = side_effect
        cfg = PipelineConfig(output_dir=tmp_path / "out", log_dir=tmp_path / "logs")
        dblp = tmp_path / "dblp.xml.gz"
        dblp.touch()
        cfg.dblp_file = dblp

        result = run_pipeline(cfg)
        assert result is True
