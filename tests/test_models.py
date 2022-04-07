import pytest
from pydantic import ValidationError

from pipeline.models import Pipeline


def test_duplicated_commands():
    data = {
        "version": "3",
        "expectations": {"population_size": 10},
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
            "action2": {
                "run": "test",
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
        },
    }

    match = "Action action2 has the same 'run' command as other actions: action1"
    with pytest.raises(ValidationError, match=match):
        Pipeline(**data)


def test_success():
    data = {
        "version": "3",
        "expectations": {"population_size": 10},
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {
                    "moderately_sensitive": {"cohort": "output.csv"},
                },
            },
        },
    }

    Pipeline(**data)


def test_unknown_name_in_needs():
    data = {
        "version": "3",
        "expectations": {"population_size": 10},
        "actions": {
            "action1": {
                "run": "test",
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
