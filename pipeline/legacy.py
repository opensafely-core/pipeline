import dataclasses
import shlex

from .exceptions import ProjectValidationError, YAMLError
from .extractors import is_extraction_command
from .main import load_pipeline


class UnknownActionError(ProjectValidationError):
    pass


# Tiny dataclass to capture the specification of a project action
@dataclasses.dataclass
class ActionSpecifiction:
    run: str
    needs: list
    outputs: dict


def parse_and_validate_project_file(project_file):
    """Parse and validate the project file.

    Args:
        project_file: The contents of the project file as an immutable array of bytes.

    Returns:
        A dict representing the project.

    Raises:
        ProjectValidationError: The project could not be parsed, or was not valid
    """
    try:
        config = load_pipeline(project_file, filename="project.yaml")
    except YAMLError as e:
        raise ProjectValidationError(*e.args)

    return config.dict(exclude_unset=True)


def get_action_specification(config, action_id, using_dummy_data_backend=False):
    """Get a specification for the action from the project.

    Args:
        config: A Pipeline model representing the pipeline configuration.
        action_id: The string ID of the action.

    Returns:
        An instance of ActionSpecification.

    Raises:
        UnknownActionError: The action was not found in the project.
        ProjectValidationError: The project was not valid.
    """
    try:
        action_spec = config.actions[action_id]
    except KeyError:
        raise UnknownActionError(f"Action '{action_id}' not found in project.yaml")
    run_command = action_spec.run.run
    run_args = shlex.split(run_command)

    # Special case handling for the `cohortextractor generate_cohort` command
    if is_extraction_command(run_args, require_version=1):
        # Set the size of the dummy data population, if that's what we're
        # generating.  Possibly this should be moved to the study definition
        # anyway, which would make this unnecessary.
        if using_dummy_data_backend:
            if action_spec.dummy_data_file is not None:
                run_command += f" --dummy-data-file={action_spec.dummy_data_file}"
            else:
                size = config.expectations.population_size
                run_command += f" --expectations-population={size}"

    elif is_extraction_command(run_args, require_version=2):
        # cohortextractor Version 2 expects all command line arguments to be
        # specified in the run command
        if using_dummy_data_backend and "--dummy-data-file" not in run_command:
            raise ProjectValidationError(
                "--dummy-data-file is required for a local run"
            )

    # TODO: we can probably remove this fork since the v1&2 forks cover it
    elif is_extraction_command(run_args):  # pragma: no cover
        raise RuntimeError("Unhandled cohortextractor version")

    return ActionSpecifiction(
        run=run_command,
        needs=action_spec.needs,
        outputs=action_spec.outputs.dict(exclude_unset=True),
    )


def get_all_output_patterns_from_project_file(project_file):
    project = parse_and_validate_project_file(project_file)
    all_patterns = set()
    for action in project["actions"].values():
        for patterns in action["outputs"].values():
            all_patterns.update(patterns.values())
    return list(all_patterns)
