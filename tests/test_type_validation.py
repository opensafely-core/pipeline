import re

import pytest

from pipeline.exceptions import ValidationError
from pipeline.models import Pipeline


def test_missing_actions(version):
    with pytest.raises(
        ValidationError, match="Project `actions` section must be a dictionary"
    ):
        Pipeline.build(version=version)


def test_actions_incorrect_type(version):
    with pytest.raises(
        ValidationError, match="Project `actions` section must be a dictionary"
    ):
        Pipeline.build(version=version, actions=[])


def test_outputs_incorrect_type(version):
    with pytest.raises(
        ValidationError,
        match="`outputs` section for action action1 must be a dictionary",
    ):
        Pipeline.build(
            version=version,
            actions={"action1": {"outputs": [], "run": "test:v1"}},
        )


def test_run_incorrect_type(version):
    with pytest.raises(
        ValidationError, match="`run` section for action action1 must be a string"
    ):
        Pipeline.build(
            version=version,
            actions={"action1": {"outputs": {}, "run": ["test:v1"]}},
        )


def test_needs_incorrect_type(version):
    with pytest.raises(
        ValidationError, match="`needs` section for action action1 must be a list"
    ):
        Pipeline.build(
            version=version,
            actions={"action1": {"outputs": {}, "run": "test:v1", "needs": ""}},
        )


def test_config_incorrect_type(version):
    with pytest.raises(
        ValidationError,
        match="`config` section for action action1 must be a dictionary",
    ):
        Pipeline.build(
            version=version,
            actions={"action1": {"outputs": {}, "run": "test:v1", "config": []}},
        )


def test_dummy_data_file_incorrect_type(version):
    with pytest.raises(
        ValidationError,
        match="`dummy_data_file` section for action action1 must be a string",
    ):
        Pipeline.build(
            version=version,
            actions={
                "action1": {"outputs": {}, "run": "test:v1", "dummy_data_file": []}
            },
        )


def test_output_files_incorrect_type(version):
    with pytest.raises(
        ValidationError,
        match="`highly_sensitive` section for action action1 must be a dictionary",
    ):
        Pipeline.build(
            version=version,
            actions={
                "action1": {"outputs": {"highly_sensitive": []}, "run": "test:v1"}
            },
        )


def test_output_filename_incorrect_type(version):
    with pytest.raises(
        ValidationError,
        match="`dataset` output for action action1 must be a string",
    ):
        Pipeline.build(
            version=version,
            actions={
                "action1": {
                    "outputs": {"highly_sensitive": {"dataset": {}}},
                    "run": "test:v1",
                }
            },
        )


def test_project_extra_parameters(version):
    with pytest.raises(
        ValidationError, match=re.escape("Unexpected parameters (extra) in project")
    ):
        Pipeline.build(version=version, extra=123)


def test_action_extra_parameters(version):
    with pytest.raises(
        ValidationError,
        match=re.escape("Unexpected parameters (extra) in action action1"),
    ):
        Pipeline.build(
            version=version,
            actions={
                "action1": {
                    "outputs": {},
                    "run": "test:v1",
                    "extra": 123,
                }
            },
        )


def test_outputs_extra_parameters(version):
    with pytest.raises(
        ValidationError,
        match=re.escape(
            "Unexpected parameters (extra) in `outputs` section for action action1"
        ),
    ):
        Pipeline.build(
            version=version,
            actions={
                "action1": {
                    "outputs": {"highly_sensitive": {"dataset": {}}, "extra": 123},
                    "run": "test:v1",
                }
            },
        )
