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


def test_no_outputs_specified():
    data = {
        "version": "3",
        "expectations": {"population_size": 10},
        "actions": {
            "action1": {
                "run": "test",
                "outputs": {},
            },
        },
    }

    match = "must specify at least one output of: highly_sensitive, moderately_sensitive, minimally_sensitive"
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
