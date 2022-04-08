import pytest
from pydantic import ValidationError

from pipeline.models import Pipeline


def test_success():
    data = {
        "version": "3",
        "expectations": {"population_size": 10},
        "actions": {
            "action1": {
                "run": "test:latest",
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
        },
    }

    Pipeline(**data)


def test_action_has_a_version():
    data = {
        "version": 1,
        "actions": {
            "generate_cohort": {
                "run": "test foo",
                "outputs": {
                    "highly_sensitive": {"cohort": "output/input.csv"},
                },
            }
        },
    }

    msg = "test must have a version specified"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_action_cohortextractor_multiple_outputs_with_output_flag():
    data = {
        "version": 1,
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

    run_command = Pipeline(**data).actions["generate_cohort"].run.run

    assert run_command == "cohortextractor:latest generate_cohort --output-dir=output"


def test_action_cohortextractor_multiple_ouputs_without_output_flag():
    data = {
        "version": 1,
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

    msg = (
        "generate_cohort command should produce output in only one directory, found 2:"
    )
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_action_extraction_command_with_multiple_outputs():
    data = {
        "version": 1,
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

    msg = "A `generate_cohort` action must have exactly one output"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_action_extraction_command_with_one_outputs():
    data = {
        "version": 1,
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {
                    "highly_sensitive": {"cohort": "output/input.csv"},
                },
            }
        },
    }

    config = Pipeline(**data)

    outputs = config.actions["generate_cohort"].outputs.dict(exclude_unset=True)
    assert len(outputs.values()) == 1


def test_action_with_config():
    data = {
        "version": 1,
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

    my_action = Pipeline(**data).actions["my_action"]

    assert my_action.config == {"my_key": "my_value"}
    assert (
        my_action.run.run
        == """python:latest python analysis/my_action.py --config '{"my_key": "my_value"}'"""
    )


def test_expectations_before_v3_has_a_default_set():
    data = {
        "version": 2,
        "expectations": {},
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    config = Pipeline(**data)

    assert config.expectations.population_size == 1000


def test_expectations_exists():
    # our logic for this is custom so ensure it works as expected
    data = {
        "version": 3,
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    msg = "Project must include `expectations` section"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_expectations_population_size_exists():
    data = {
        "version": 3,
        "expectations": {},
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    msg = "Project `expectations` section must include `population_size` section"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_expectations_population_size_is_a_number():
    data = {
        "version": 3,
        "expectations": {"population_size": "test"},
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }

    msg = "Project expectations population size must be a number"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_pipeline_needs_success():
    data = {
        "version": 1,
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

    config = Pipeline(**data)

    assert config.actions["do_analysis"].needs == ["generate_cohort"]


def test_pipeline_needs_with_non_comma_delimited_actions():
    data = {
        "version": 1,
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
            },
            "do_analysis": {
                "run": "python:latest foo.py",
                "outputs": {"moderately_sensitive": {"cohort2": "output/input2.csv"}},
            },
            "do_further_analysis": {
                "run": "python:latest foo2.py",
                "needs": ["generate_cohort do_analysis"],
                "outputs": {"moderately_sensitive": {"cohort3": "output/input3.csv"}},
            },
        },
    }

    msg = "`needs` actions should be separated with commas. The following actions need fixing:"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_pipeline_needs_with_unknown_action():
    data = {
        "version": 1,
        "actions": {
            "action1": {
                "run": "test:latest",
                "needs": ["action2"],
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
        },
    }

    match = "One or more actions is referencing unknown actions in its needs list"
    with pytest.raises(ValidationError, match=match):
        Pipeline(**data)


def test_pipeline_with_duplicated_action_run_commands():
    data = {
        "version": 1,
        "actions": {
            "action1": {
                "run": "test:lastest",
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
            "action2": {
                "run": "test:lastest",
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
        },
    }

    match = "Action action2 has the same 'run' command as other actions: action1"
    with pytest.raises(ValidationError, match=match):
        Pipeline(**data)


def test_pipeline_with_missing_or_none_version():
    data = {
        "expectations": {"population_size": 10},
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {"highly_sensitive": {"cohort": "output.csv"}},
            },
        },
    }

    msg = "Project file must have a `version` attribute"

    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_pipeline_with_non_numeric_version():
    data = {
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {"highly_sensitive": {"cohort": "output.csv"}},
            },
        },
    }

    msg = "`version` must be a number between 1 and"

    with pytest.raises(ValidationError, match=msg):
        data["version"] = "test"
        Pipeline(**data)

    with pytest.raises(ValidationError, match=msg):
        data["version"] = None
        Pipeline(**data)


def test_outputs_files_are_unique():
    data = {
        "version": 2,
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

    msg = "Output path output/input.csv is not unique"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


def test_outputs_duplicate_files_in_v1():
    data = {
        "version": 1,
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

    generate_cohort = Pipeline(**data).actions["generate_cohort"]

    cohort = generate_cohort.outputs.highly_sensitive["cohort"]
    test = generate_cohort.outputs.highly_sensitive["test"]

    assert cohort == test


def test_outputs_with_unknown_privacy_level():
    msg = "must specify at least one output of: highly_sensitive, moderately_sensitive, minimally_sensitive"

    with pytest.raises(ValidationError, match=msg):
        # no outputs
        Pipeline(
            **{
                "version": 1,
                "actions": {
                    "action1": {
                        "run": "test",
                        "outputs": {},
                    },
                },
            }
        )

    with pytest.raises(ValidationError, match=msg):
        Pipeline(
            **{
                "version": 1,
                "actions": {
                    "action1": {
                        "run": "test",
                        "outputs": {"test": {"cohort": "output/input.csv"}},
                    }
                },
            }
        )


def test_outputs_with_invalid_pattern():
    data = {
        "version": 1,
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"highly_sensitive": {"test": "test?foo"}},
            },
        },
    }

    msg = "Output path test\\?foo is not permitted:"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)


@pytest.mark.parametrize("image", ["cohortextractor-v2", "databuilder"])
def test_pipeline_databuilder_specifies_output(image):
    data = {
        "version": 1,
        "actions": {
            "generate_cohort_v2": {
                "run": f"{image}:latest generate_cohort --output=output/cohort1.csv",
                "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
            }
        },
    }

    msg = "--output in run command and outputs must match"
    with pytest.raises(ValidationError, match=msg):
        Pipeline(**data)
