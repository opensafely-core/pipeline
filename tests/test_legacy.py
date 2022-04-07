import argparse
import shlex

import pytest

from pipeline.legacy import (
    InvalidPatternError,
    ProjectValidationError,
    UnknownActionError,
    add_config_to_run_command,
    args_include,
    assert_valid_glob_pattern,
    get_action_specification,
    get_all_actions,
    get_all_output_patterns_from_project_file,
    is_generate_cohort_command,
    parse_and_validate_project_file,
    validate_project_and_set_defaults,
)
from pipeline.models import Pipeline


def test_add_config_to_run_command_with_option():
    command = add_config_to_run_command(
        "python:latest python analysis/my_action.py --option value",
        {"option": "value"},
    )

    assert (
        command
        == """python:latest python analysis/my_action.py --option value --config '{"option": "value"}'"""
    )


def test_add_config_to_run_command_with_argument():
    command = add_config_to_run_command(
        "python:latest python action/__main__.py output/input.csv",
        {"option": "value"},
    )

    assert (
        command
        == """python:latest python action/__main__.py output/input.csv --config '{"option": "value"}'"""
    )

    # Does argparse accept options after arguments?
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")  # option
    parser.add_argument("input_files", nargs="*")  # argument

    # If parser were in __main__.py, then parser.parse_args would receive sys.argv
    # by default. sys.argv[0] is the script name (either with or without a path,
    # depending on the OS) so we slice obs_run_command to mimic this.
    parser.parse_args(shlex.split(command)[2:])


def test_args_include():
    assert not args_include(["foo=test"], "test")
    assert not args_include([], "test")

    assert args_include(["test=test", "test", "foo", "bar"], "test")


def test_assert_valid_glob_pattern():
    assert_valid_glob_pattern("foo/bar/*.txt")
    assert_valid_glob_pattern("foo")
    bad_patterns = [
        "/abs/path",
        "ends/in/slash/",
        "not//canonical",
        "path/../traversal",
        "c:/windows/absolute",
        "recursive/**/glob.pattern",
        "questionmark?",
        "/[square]brackets",
        "\\ftest",
        "metadata",
        "metadata/test",
    ]
    for pattern in bad_patterns:
        with pytest.raises(InvalidPatternError):
            assert_valid_glob_pattern(pattern)


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
        == """cohortextractor:latest generate_cohort --expectations-population=1000 --output-dir=output"""
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


def test_get_action_specification_multiple_ouputs_with_output_flag():
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort --output-dir=output",
                    "outputs": {
                        "moderately_sensitive": {
                            "cohort": "output/input.csv",
                            "other": "other/graph.png",
                        }
                    },
                }
            },
        }
    ).dict(exclude_unset=True)

    action_spec = get_action_specification(project_dict, "generate_cohort")

    assert (
        action_spec.run == "cohortextractor:latest generate_cohort --output-dir=output"
    )


def test_get_action_specification_multiple_ouputs_without_output_flag():
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {
                        "moderately_sensitive": {
                            "cohort": "output/input.csv",
                            "other": "other/graph.png",
                        }
                    },
                }
            },
        }
    ).dict(exclude_unset=True)

    msg = (
        "^generate_cohort command should produce output in only one directory, found 2:"
    )
    with pytest.raises(ProjectValidationError, match=msg):
        get_action_specification(project_dict, "generate_cohort")


def test_get_action_specification_with_config():
    project_dict = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1_000},
            "actions": {
                "my_action": {
                    "run": "python:latest python analysis/my_action.py",
                    "config": {"my_key": "my_value"},
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
        == """python:latest python analysis/my_action.py --config '{"my_key": "my_value"}'"""
    )


def test_get_action_specification_with_dummy_data_file_flag():
    # TODO: support Action.dummy_data_file: Path in model
    project_dict = {
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
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
        == "cohortextractor:latest generate_cohort --dummy-data-file=test --output-dir=output"
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


@pytest.mark.parametrize(
    "require_version,args,desired_outcome",
    [
        (1, ["cohortextractor:latest", "generate_cohort"], True),
        (1, ["cohortextractor-v2:latest", "generate_cohort"], False),
        (1, ["databuilder:latest", "generate_dataset"], False),
        (2, ["cohortextractor:latest", "generate_cohort"], False),
        (2, ["cohortextractor-v2:latest", "generate_cohort"], True),
        (2, ["databuilder:latest", "generate_dataset"], True),
    ],
)
def test_is_generate_cohort_command_with_version(
    args, require_version, desired_outcome
):
    output = is_generate_cohort_command(args, require_version=require_version)

    assert output == desired_outcome


@pytest.mark.parametrize(
    "args,desired_outcome",
    [
        (["cohortextractor:latest", "generate_cohort"], True),
        (["cohortextractor-v2:latest", "generate_cohort"], True),
        (["databuilder:latest", "generate_dataset"], True),
        (["test"], False),
        (["test", "generate_cohort"], False),
        (["test", "generate_dataset"], False),
    ],
)
def test_is_generate_cohort_command_without_version(args, desired_outcome):
    assert is_generate_cohort_command(args) == desired_outcome


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


def test_validate_project_and_set_defaults_action_has_a_version():
    project_dict = Pipeline(
        **{
            "version": "2",
            "expectations": {"population_size": 1_000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor generate_cohort",
                    "outputs": {
                        "highly_sensitive": {"cohort": "output/input.csv"},
                    },
                }
            },
        }
    ).dict(exclude_unset=True)

    msg = "^cohortextractor must have a version specified"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_action_command_is_unique():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover action commands
    # being unique
    project_dict = {
        "version": 2,
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
            },
            "generate_cohort2": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"moderately_sensitive": {"cohort2": "output/input2.csv"}},
            },
        },
    }

    msg = "^cohortextractor generate_cohort appears more than once"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_action_needs_success():
    project_dict = Pipeline(
        **{
            "version": "3",
            "expectations": {"population_size": 1_000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
                },
                "do_analysis": {
                    "run": "python:latest foo.py",
                    "outputs": {"highly_sensitive": {"cohort2": "output/input2.csv"}},
                    "needs": ["generate_cohort"],
                },
            },
        }
    ).dict(exclude_unset=True)

    config = validate_project_and_set_defaults(project_dict)

    assert config["actions"]["do_analysis"]["needs"] == ["generate_cohort"]


def test_validate_project_and_set_defaults_action_needs_with_non_comma_delimited_actions():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover the permitted
    # privacy levels
    project_dict = {
        "version": 2,
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
            },
            "do_analysis": {
                "run": {
                    "run": "python:latest foo.py",
                    "name": "foo.py",
                    "version": "latest",
                    "args": "",
                },
                "outputs": {"moderately_sensitive": {"cohort2": "output/input2.csv"}},
            },
            "do_further_analysis": {
                "run": {
                    "run": "python:latest foo2.py",
                    "name": "foo2.py",
                    "version": "latest",
                    "args": "",
                },
                "needs": ["generate_cohort do_analysis"],
                "outputs": {"moderately_sensitive": {"cohort3": "output/input3.csv"}},
            },
        },
    }

    msg = "^`needs` actions in 'do_further_analysis' should be separated with commas"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_action_needs_with_unknown_action():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover an unknown action in
    # the needs key
    project_dict = {
        "version": 2,
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
            },
            "do_analysis": {
                "run": {
                    "run": "python:latest foo.py",
                    "name": "foo.py",
                    "version": "latest",
                    "args": "",
                },
                "needs": ["unknown"],
                "outputs": {"moderately_sensitive": {"cohort2": "output/input2.csv"}},
            },
        },
    }

    msg = "^Action 'do_analysis' lists unknown action 'unknown' in its `needs` config$"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_expectations_before_v3_has_a_default_set():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover the shape and
    # required value of expections and expectations.population_size
    project_dict = {
        "version": 2,
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    project = validate_project_and_set_defaults(project_dict)

    assert project["expectations"]["population_size"] == 1000


def test_validate_project_and_set_defaults_expectations_exists():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover the shape and
    # required value of expections and expectations.population_size
    project_dict = {
        "version": 3,
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    msg = "^Project must include `expectations` section$"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_expectations_population_size_exists():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover the shape and
    # required value of expections and expectations.population_size
    project_dict = {
        "version": 3,
        "expectations": {},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    msg = "^Project `expectations` section must include `population` section$"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_expectations_population_size_is_a_number():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover the shape and
    # required value of expections and expectations.population_size
    project_dict = {
        "version": 3,
        "expectations": {"population_size": "test"},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    msg = "^Project expectations population size must be a number$"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_generate_cohort_has_only_one_output():
    project_dict = Pipeline(
        **{
            "version": "2",
            "expectations": {"population_size": 1_000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {
                        "highly_sensitive": {"cohort": "output/input.csv"},
                        "moderately_sensitive": {"cohort2": "output/input2.csv"},
                    },
                }
            },
        }
    ).dict(exclude_unset=True)

    msg = "^A `generate_cohort` action must have exactly one output"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_generate_cohort_with_unknown_privacy_level():
    # TODO: remove this test and the privacy level check when we start removing
    # bits of legacy.py, the pydantic models already cover the permitted
    # privacy levels
    project_dict = {
        "version": 2,
        "expectations": {"population_size": 1000},
        "actions": {
            "generate_cohort": {
                "run": {
                    "run": "cohortextractor:latest generate_cohort",
                    "name": "cohortextractor",
                    "version": "latest",
                    "args": "generate_cohort",
                },
                "outputs": {"test": {"cohort": "output/input.csv"}},
            }
        },
    }

    with pytest.raises(ProjectValidationError, match="^test is not valid"):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_with_invalid_pattern():
    project_dict = Pipeline(
        **{
            "version": "3",
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {"highly_sensitive": {"test": "test?foo"}},
                },
            },
        }
    ).dict(exclude_unset=True)

    msg = "^Output path test\\?foo is not permitted:"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)


def test_validate_project_and_set_defaults_with_duplicate_output_files():
    project_dict = Pipeline(
        **{
            "version": "3",
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {
                        "highly_sensitive": {
                            "cohort": "output/input.csv",
                            "test": "output/input.csv",
                        }
                    },
                },
            },
        }
    ).dict(exclude_unset=True)

    msg = "^Output path output/input.csv is not unique"
    with pytest.raises(ProjectValidationError, match=msg):
        validate_project_and_set_defaults(project_dict)
