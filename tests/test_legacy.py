import argparse
import shlex

import pytest

from pipeline.legacy import (
    ProjectValidationError,
    UnknownActionError,
    get_action_specification,
    get_all_actions,
    get_all_output_patterns_from_project_file,
    parse_and_validate_project_file,
)
from pipeline.models import Pipeline


def test_get_action_specification_databuilder_has_output_flag():
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_dataset": {
                    "run": "databuilder:latest generate_dataset --output=output/dataset.csv",
                    "outputs": {
                        "highly_sensitive": {
                            "cohort": "output/dataset.csv",
                            "cohort2": "output/input2.csv",
                        }
                    },
                },
            },
        }
    ).dict(exclude_unset=True)

    action_spec = get_action_specification(project_dict, "generate_dataset")

    assert (
        action_spec.run
        == "databuilder:latest generate_dataset --output=output/dataset.csv"
    )


def test_get_action_specification_for_cohortextractor_generate_cohort_action():
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
                }
            },
        }
    ).dict(exclude_unset=True)

    action_spec = get_action_specification(
        project_dict, "generate_cohort", using_dummy_data_backend=True
    )

    assert (
        action_spec.run
        == """cohortextractor:latest generate_cohort --output-dir=output --expectations-population=1000"""
    )


@pytest.mark.parametrize("image", ["cohortextractor-v2", "databuilder"])
def test_get_action_specification_for_databuilder_action(image):
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort_v2": {
                    "run": f"{image}:latest generate_cohort --output=output/cohort.csv --dummy-data-file dummy.csv",
                    "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
                }
            },
        }
    ).dict(exclude_unset=True)

    action_spec = get_action_specification(
        project_dict, "generate_cohort_v2", using_dummy_data_backend=True
    )

    assert (
        action_spec.run
        == f"""{image}:latest generate_cohort --output=output/cohort.csv --dummy-data-file dummy.csv"""
    )


@pytest.mark.parametrize(
    "args,error,image",
    [
        (
            "--output=output/cohort1.csv --dummy-data-file dummy.csv",
            "--output in run command and outputs must match",
            "cohortextractor-v2",
        ),
        (
            "--output=output/cohort1.csv",
            "--dummy-data-file is required for a local run",
            "cohortextractor-v2",
        ),
        (
            "--output=output/cohort1.csv --dummy-data-file dummy.csv",
            "--output in run command and outputs must match",
            "databuilder",
        ),
        (
            "--output=output/cohort1.csv",
            "--dummy-data-file is required for a local run",
            "databuilder",
        ),
    ],
)
def test_get_action_specification_for_databuilder_errors(args, error, image):
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1_000},
            "actions": {
                "generate_cohort_v2": {
                    "run": f"{image}:latest generate_cohort {args}",
                    "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
                }
            },
        }
    ).dict(exclude_unset=True)
    action_id = "generate_cohort_v2"
    with pytest.raises(ProjectValidationError, match=error):
        get_action_specification(project_dict, action_id, using_dummy_data_backend=True)


def test_get_action_specification_with_config():
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1_000},
            "actions": {
                "my_action": {
                    "run": "python:latest python action/__main__.py output/input.csv",
                    "config": {"option": "value"},
                    "outputs": {
                        "moderately_sensitive": {"my_figure": "output/my_figure.png"}
                    },
                }
            },
        }
    ).dict(exclude_unset=True)

    action_spec = get_action_specification(project_dict, "my_action")

    assert (
        action_spec.run
        == """python:latest python action/__main__.py output/input.csv --config '{"option": "value"}'"""
    )

    # Does argparse accept options after arguments?
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")  # option
    parser.add_argument("input_files", nargs="*")  # argument

    # If parser were in __main__.py, then parser.parse_args would receive sys.argv
    # by default. sys.argv[0] is the script name (either with or without a path,
    # depending on the OS) so we slice obs_run_command to mimic this.
    parser.parse_args(shlex.split(action_spec.run)[2:])


def test_get_action_specification_with_dummy_data_file_flag():
    # TODO: support Action.dummy_data_file: Path in model
    project_dict = {
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort --output-dir=output",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
                "dummy_data_file": "test",
            }
        },
    }

    action_spec = get_action_specification(
        project_dict,
        "generate_cohort",
        using_dummy_data_backend=True,
    )

    assert (
        action_spec.run
        == "cohortextractor:latest generate_cohort --output-dir=output --dummy-data-file=test"
    )


def test_get_action_specification_without_dummy_data_file_flag():
    project_dict = {
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort --output-dir=output",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
                "dummy_data_file": "test",
            }
        },
    }

    action_spec = get_action_specification(project_dict, "generate_cohort")

    assert (
        action_spec.run == "cohortextractor:latest generate_cohort --output-dir=output"
    )


def test_get_action_specification_with_unknown_action():
    project_dict = {"actions": {"known_action": {}}}

    with pytest.raises(UnknownActionError):
        get_action_specification(project_dict, "unknown_action")


def test_get_all_actions():
    config = {
        "actions": {
            "first": {},
            "second": {},
            "run_all": {},
        }
    }
    assert get_all_actions(config) == ["first", "second"]

    assert get_all_actions({"actions": {}}) == []


def test_get_all_output_patterns_from_project_file_success(mocker):
    config = {
        "version": 3,
        "expectations": {"population_size": 1000},
        "actions": {
            "first": {
                "outputs": {
                    "moderately_sensitive": {
                        "cohort": "output/input.csv",
                        "other": "output/graph_*.png",
                    },
                },
            },
            "second": {
                "outputs": {
                    "moderately_sensitive": {
                        "second": "output/*.csv",
                    },
                },
            },
        },
    }

    mocker.patch(
        "pipeline.legacy.parse_and_validate_project_file",
        auto_spec=True,
        return_value=config,
    )

    patterns = get_all_output_patterns_from_project_file("test")

    assert set(patterns) == {"output/input.csv", "output/graph_*.png", "output/*.csv"}


def test_get_all_output_patterns_from_project_file_with_no_outputs(mocker):
    config = {
        "version": 3,
        "expectations": {"population_size": 1000},
        "actions": {"first": {"outputs": {}}},
    }

    mocker.patch(
        "pipeline.legacy.parse_and_validate_project_file",
        auto_spec=True,
        return_value=config,
    )

    assert get_all_output_patterns_from_project_file("") == []


def test_parse_and_validate_project_file_with_action():
    project_file = """
    version: '3.0'
    expectations:
      population_size: 1000
    actions:
      my_action:
        run: python:latest python analysis/my_action.py
        outputs:
          moderately_sensitive:
            my_figure: output/my_figure.png
    """
    config = parse_and_validate_project_file(project_file)

    command = config["actions"]["my_action"]["run"]["run"]
    assert command == "python:latest python analysis/my_action.py"


def test_parse_and_validate_project_file_with_duplicate_keys():
    project_file = """
        top_level:
            duplicate: 1
            duplicate: 2
    """
    msg = 'found duplicate key "duplicate" with value "2"'
    with pytest.raises(ProjectValidationError, match=msg):
        parse_and_validate_project_file(project_file)
