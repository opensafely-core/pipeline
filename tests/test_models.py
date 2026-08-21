import dataclasses

import pytest

from pipeline import load_pipeline
from pipeline.exceptions import ValidationError
from pipeline.features import LATEST_VERSION, MINIMUM_VERSION
from pipeline.models import Outputs, Pipeline


def test_success():
    data = {
        "version": "4",
        "actions": {
            "action1": {
                "run": "test:latest",
                "outputs": {
                    "moderately_sensitive": {"dataset": "output.csv"},
                },
            },
        },
    }

    Pipeline.build(**data)


@pytest.mark.parametrize(
    "action",
    [
        "test",
        "test:",
        "test:v",
        "test:other",
        "test:vnotdigits",
        "test:v1x1",
        "test:pre",
    ],
)
def test_action_handles_invalid_version(action):
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": action,
                "outputs": {
                    "highly_sensitive": {"output": "output/input.csv"},
                },
            }
        },
    }

    msg = "test must have a version specified"
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


@pytest.mark.parametrize(
    "action",
    [
        "test:v1",
        "test:v2",
        "test:v1.2",
        "test:v1.2.3",
        "test:dev",
        "test:latest",
        "test:v1-pre",
        "test:v1.2-pre",
    ],
)
def test_action_handles_valid_version(action):
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": action,
                "outputs": {
                    "highly_sensitive": {"output": "output/input.csv"},
                },
            }
        },
    }

    run = Pipeline.build(**data).actions["generate_output"].run
    n, _, v = action.partition(":")
    assert run.name == n
    assert run.version == v


@pytest.mark.parametrize("image", ["databuilder", "ehrql"])
@pytest.mark.parametrize("sensitivity", ["moderately_sensitive", "minimally_sensitive"])
def test_action_extraction_command_with_less_than_highly_sensitive_output(
    image, sensitivity
):
    data = {
        "version": 4,
        "actions": {
            "generate_dataset": {
                "run": f"{image}:latest generate-dataset",
                "outputs": {
                    sensitivity: {"dataset": "output/input.csv"},
                },
            }
        },
    }

    msg = (
        "`generate_dataset` action uses `generate-dataset` and so all outputs must "
        "be labelled `highly_sensitive`"
    )
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


def test_expected_privacy_levels():
    # If these levels ever change then the `sensitivity` parameters above need to change
    # to reflect them and remain exhaustive
    assert {f.name for f in dataclasses.fields(Outputs)} == {
        "highly_sensitive",
        "moderately_sensitive",
        "minimally_sensitive",
    }


def test_action_ehrql_with_no_output_file():
    data = {
        "version": 4,
        "actions": {
            "generate_dataset": {
                "run": "ehrql:v1 generate-dataset",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/input.csv"},
                },
            }
        },
    }

    msg = (
        "`generate_dataset` action does not provide an `--output` argument specifying "
        "where the results of `generate-dataset` should be stored"
    )
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


def test_action_extraction_command_with_one_outputs():
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": "action:v1 generate_output",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/input.csv"},
                },
            }
        },
    }

    config = Pipeline.build(**data)

    outputs = config.actions["generate_output"].outputs
    assert len(outputs) == 1


def test_action_ehrql_with_multiple_output_files():
    data = {
        "version": 4,
        "actions": {
            "generate_dataset": {
                "run": "ehrql:v1 generate-dataset --output outputs:arrow",
                "outputs": {
                    "highly_sensitive": {"results": "outputs/*.arrow"},
                },
            }
        },
    }

    assert Pipeline.build(**data)


def test_action_ehrql_with_multiple_output_files_and_mismatch():
    data = {
        "version": 4,
        "actions": {
            "generate_dataset": {
                "run": "ehrql:v1 generate-dataset --output outputs:arrow",
                "outputs": {
                    "highly_sensitive": {"results": "outputs.arrow"},
                },
            }
        },
    }
    msg = (
        "(?s)"
        "output specification must match the `--output` argument in the `run` command"
        ".*got: outputs.arrow"
        ".*but was expecting: outputs/:arrow"
    )
    with pytest.raises(ValidationError, match=msg):
        assert Pipeline.build(**data)


def test_cohortextractor_actions_not_used_after_v3():
    data = {
        "version": "4",
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:v1 generate_cohort",
                "outputs": {
                    "highly_sensitive": {"cohort": "output/input.csv"},
                },
            },
        },
    }
    msg = "uses cohortextractor actions, which are not supported in this version."
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


def test_command_properties():
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": "action:v1 generate_output another_arg",
                "outputs": {"highly_sensitive": {"output": "output/input.csv"}},
            }
        },
    }

    action = Pipeline.build(**data).actions["generate_output"]
    assert action.run.args == "generate_output another_arg"
    assert action.run.name == "action"
    assert action.run.parts == [
        "action:v1",
        "generate_output",
        "another_arg",
    ]
    assert action.run.version == "v1"


def test_pipeline_all_actions(test_file):
    # load the pipeline fixture for simplicity here
    config = load_pipeline(test_file)

    assert config.all_actions == [
        "generate_dataset",
        "prepare_data_m",
        "prepare_data_f",
        "prepare_data_with_quote_in_filename",
        "analyse_data",
        "test_reusable_action",
        "test_cancellation",
    ]


def test_pipeline_needs_success():
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": "action:v1 generate_output",
                "outputs": {"highly_sensitive": {"output": "output/input.csv"}},
            },
            "do_analysis": {
                "run": "python:latest foo.py",
                "outputs": {"highly_sensitive": {"output2": "output/input2.csv"}},
                "needs": ["generate_output"],
            },
        },
    }

    config = Pipeline.build(**data)

    assert config.actions["do_analysis"].needs == ["generate_output"]


def test_pipeline_needs_with_non_comma_delimited_actions():
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": "action:v1 generate_output",
                "outputs": {"moderately_sensitive": {"output": "output/input.csv"}},
            },
            "do_analysis": {
                "run": "python:latest foo.py",
                "outputs": {"moderately_sensitive": {"output2": "output/input2.csv"}},
            },
            "do_further_analysis": {
                "run": "python:latest foo2.py",
                "needs": ["generate_output do_analysis"],
                "outputs": {"moderately_sensitive": {"output3": "output/input3.csv"}},
            },
        },
    }

    msg = "`needs` actions should be separated with commas, but do_further_analysis needs `generate_output do_analysis`"
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


def test_pipeline_needs_with_unknown_action():
    data = {
        "version": 4,
        "actions": {
            "action1": {
                "run": "test:latest",
                "needs": ["action2"],
                "outputs": {
                    "moderately_sensitive": {"dataset": "output.csv"},
                },
            },
        },
    }

    match = "Action `action1` references an unknown action in its `needs` list: action2"
    with pytest.raises(ValidationError, match=match):
        Pipeline.build(**data)


def test_pipeline_with_duplicated_action_run_commands():
    data = {
        "version": 4,
        "actions": {
            "action1": {
                "run": "test:latest",
                "outputs": {
                    "moderately_sensitive": {"dataset": "output.csv"},
                },
            },
            "action2": {
                "run": "test:latest",
                "outputs": {
                    "moderately_sensitive": {"dataset": "output.csv"},
                },
            },
        },
    }

    match = "Action action2 has the same 'run' command as other actions: action1"
    with pytest.raises(ValidationError, match=match):
        Pipeline.build(**data)


@pytest.mark.parametrize(
    "action_value,match",
    [
        (None, "Configuration for action action1 must be a dictionary"),
        ({}, "Action action1 must contain a configuration for 'run'"),
    ],
)
def test_pipeline_with_empty_action(action_value, match):
    data = {
        "version": 4,
        "actions": {"action1": action_value},
    }
    with pytest.raises(ValidationError, match=match):
        Pipeline.build(**data)


def test_pipeline_with_empty_run_command():
    data = {
        "version": 4,
        "actions": {
            "action1": {
                "run": "",
                "outputs": {
                    "moderately_sensitive": {"dataset": "output.csv"},
                },
            },
        },
    }

    match = "run must have a value, action1 has an empty run key"
    with pytest.raises(ValidationError, match=match):
        Pipeline.build(**data)


def test_pipeline_without_specifying_output_for_action():
    data = {
        "version": 4,
        "actions": {
            "action1": {"run": "test"},
        },
    }

    match = "Action action1 must contain a configuration for 'outputs'"
    with pytest.raises(ValidationError, match=match):
        Pipeline.build(**data)


def test_pipeline_with_missing_or_none_version():
    data = {
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {"highly_sensitive": {"dataset": "output.csv"}},
            },
        },
    }

    msg = "Project file must have a `version` attribute"

    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)

    with pytest.raises(ValidationError, match=msg):
        data["version"] = None
        Pipeline.build(**data)


def test_pipeline_with_non_numeric_version():
    data = {
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {"highly_sensitive": {"output": "output.csv"}},
            },
        },
    }

    msg = f"`version` must be a number between {MINIMUM_VERSION} and {LATEST_VERSION}"

    with pytest.raises(ValidationError, match=msg):
        data["version"] = "test"
        Pipeline.build(**data)


def test_outputs_files_are_unique():
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": "action:v1 generate_output",
                "outputs": {
                    "highly_sensitive": {
                        "output": "output/input.csv",
                        "test": "output/input.csv",
                    }
                },
            },
        },
    }

    msg = "Output path output/input.csv is not unique"
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


def test_outputs_with_unknown_privacy_level():
    msg = "must specify at least one output of: highly_sensitive, moderately_sensitive, minimally_sensitive"

    with pytest.raises(ValidationError, match=msg):
        # no outputs
        Pipeline.build(
            **{
                "version": 4,
                "actions": {
                    "action1": {
                        "run": "test",
                        "outputs": {},
                    },
                },
            }
        )

    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(
            **{
                "version": 4,
                "actions": {
                    "action1": {
                        "run": "test",
                        "outputs": {"test": {"dataset": "output/input.csv"}},
                    }
                },
            }
        )


def test_outputs_with_invalid_pattern():
    data = {
        "version": 4,
        "actions": {
            "generate_output": {
                "run": "action:v1 generate_output",
                "outputs": {"highly_sensitive": {"test": "test/foo"}},
            },
        },
    }

    msg = "Output path test/foo is invalid:"
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


@pytest.mark.parametrize("image,tag", [("databuilder", "latest"), ("ehrql", "v1")])
def test_pipeline_ehrql_specifies_same_output(image, tag):
    data = {
        "version": 4,
        "actions": {
            "generate-dataset": {
                "run": f"{image}:{tag} generate-dataset --output=output/dataset.csv",
                "outputs": {"highly_sensitive": {"dataset": "output/dataset.csv"}},
            }
        },
    }

    Pipeline.build(**data)


@pytest.mark.parametrize("image,tag", [("databuilder", "latest"), ("ehrql", "v1")])
def test_pipeline_ehrql_specifies_different_output(image, tag):
    data = {
        "version": 4,
        "actions": {
            "generate-dataset": {
                "run": f"{image}:{tag} generate-dataset --output=output/dataset1.csv",
                "outputs": {"highly_sensitive": {"dataset": "output/dataset.csv"}},
            }
        },
    }

    msg = "output specification must match the `--output` argument in the `run` command"
    with pytest.raises(ValidationError, match=msg):
        Pipeline.build(**data)


def test_pipeline_databuilder_recognizes_old_action_spelling():
    # The action name is used to select the validator, so the only way to know that it's been recognized is
    # to give it an invalid input and check that validation fails.
    data = {
        "version": 4,
        "actions": {
            "old-spelling": {
                "run": "databuilder:latest generate_dataset --output=output/dataset1.csv",
                "outputs": {"highly_sensitive": {"dataset": "output/dataset.csv"}},
            }
        },
    }

    with pytest.raises(ValidationError):
        Pipeline.build(**data)


@pytest.mark.parametrize(
    "name,run,is_database_action",
    [
        (
            "generate_ehrql_dataset",
            "ehrql:v1 generate-dataset args --output=output/input.csv",
            True,
        ),
        (
            "generate_ehrql_v1_dataset",
            "ehrql:v1 generate-dataset args --output=output/input.csv",
            True,
        ),
        ("generate_ehrql_measures", "ehrql:v1 generate-measures args --option", True),
        (
            "sqlrunner",
            "sqlrunner:v1 foo -output=output/input.csv",
            True,
        ),
        (
            "generate_databuilder_dataset",
            "databuilder:v0 generate-dataset args --output=output/input.csv",
            True,
        ),
        (
            "non_db_generate_measures",
            "python:latest generate-measures.py args --option",
            False,
        ),
        ("no_command", "ehrql:v1", False),
    ],
)
def test_action_is_database_action(name, run, is_database_action):
    data = {
        "version": 4,
        "actions": {
            name: {
                "run": run,
                "outputs": {"highly_sensitive": {"outputs": "output/input.csv"}},
            }
        },
    }

    action = Pipeline.build(**data).actions[name]
    assert action.is_database_action == is_database_action


def test_action_images():
    data = {
        "version": 4,
        "actions": {
            "ehrql": {
                "run": "ehrql:v1 ...",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/ehrql.csv"},
                },
            },
            "r1": {
                "run": "r:latest 1",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/r1.csv"},
                },
            },
            "r2": {
                "run": "r:latest 2",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/r2.csv"},
                },
            },
            "python": {
                "run": "python:v2 ...",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/python.csv"},
                },
            },
        },
    }

    pipeline = Pipeline.build(**data)
    assert pipeline.action_images == set(["ehrql:v1", "r:v1", "python:v2"])


def test_action_images_v5():
    data = {
        "version": 5,
        "actions": {
            "ehrql": {
                "run": "ehrql:v1 ...",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/ehrql.csv"},
                },
            },
            "r1": {
                "run": "r:v1 1",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/r1.csv"},
                },
            },
            "r2": {
                "run": "r:v2 2",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/r2.csv"},
                },
            },
            "python": {
                "run": "python:v2 ...",
                "outputs": {
                    "highly_sensitive": {"dataset": "output/python.csv"},
                },
            },
        },
    }

    pipeline = Pipeline.build(**data)
    assert pipeline.action_images == set(["ehrql:v1", "r:v1", "r:v2", "python:v2"])


def test_run_all_action_error_in_v5():
    with pytest.raises(
        ValidationError,
        match="`run_all` is a reserved action name",
    ):
        Pipeline.build(
            version=5,
            actions={"run_all": {"outputs": {}, "run": "test:v1"}},
        )


def test_run_all_action_warning_before_v5():
    with pytest.warns(UserWarning, match="`run_all` is a reserved action name"):
        Pipeline.build(
            version=4,
            actions={
                "run_all": {
                    "outputs": {"highly_sensitive": {"foo": "bar.txt"}},
                    "run": "test:v1",
                }
            },
        )


@pytest.mark.parametrize(
    "run_command",
    [
        "ehrql:latest ...",
        "r:latest 1",
        "python:latest   do python thing",
        " reusable:latest ...",
    ],
)
def test_action_images_latest_not_allowed_in_v5(run_command):
    data = {
        "version": 5,
        "actions": {
            "my_action": {
                "run": run_command,
                "outputs": {
                    "highly_sensitive": {"output": "output/result.csv"},
                },
            },
        },
    }
    with pytest.raises(
        ValidationError,
        match=r"Action my_action uses `\w+:latest`, which is not supported. Provide a version e.g. `:v2` instead",
    ):
        Pipeline.build(**data)


def test_warning_for_old_version():
    with pytest.warns(UserWarning, match="project file is using an old version"):
        Pipeline.build(
            version=4,
            actions={
                "my_action": {
                    "outputs": {"highly_sensitive": {"foo": "bar.txt"}},
                    "run": "test:v1",
                }
            },
        )


@pytest.mark.parametrize("version", list(range(1, MINIMUM_VERSION)))
def test_deprecated_version(version):
    with pytest.raises(
        ValidationError, match="project file is using a deprecated version"
    ):
        Pipeline.build(
            version=version,
            actions={
                "my_action": {
                    "outputs": {"highly_sensitive": {"foo": "bar.txt"}},
                    "run": "test:v1",
                }
            },
        )
