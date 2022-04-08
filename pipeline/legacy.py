import dataclasses
import json
import shlex
from pathlib import PurePosixPath

from .exceptions import YAMLError
from .main import load_pipeline


# The magic action name which means "run every action"
RUN_ALL_COMMAND = "run_all"


class ProjectValidationError(Exception):
    pass


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

    project = config.dict(exclude_unset=True)
    return validate_project_and_set_defaults(project)


# Copied almost verbatim from the original job-runner
def validate_project_and_set_defaults(project):
    """Check that a dictionary of project actions is valid, and set any defaults"""
    project_actions = project["actions"]

    for action_id, action_config in project_actions.items():
        if is_generate_cohort_command(shlex.split(action_config["run"]["run"])):
            if len(action_config["outputs"]) != 1:
                raise ProjectValidationError(
                    f"A `generate_cohort` action must have exactly one output; {action_id} had {len(action_config['outputs'])}",
                )

    return project


def get_action_specification(project, action_id, using_dummy_data_backend=False):
    """Get a specification for the action from the project.

    Args:
        project: A dict representing the project.
        action_id: The string ID of the action.

    Returns:
        An instance of ActionSpecification.

    Raises:
        UnknownActionError: The action was not found in the project.
        ProjectValidationError: The project was not valid.
    """
    try:
        action_spec = project["actions"][action_id]
    except KeyError:
        raise UnknownActionError(f"Action '{action_id}' not found in project.yaml")
    run_command = action_spec["run"]["run"]
    if "config" in action_spec:
        run_command = add_config_to_run_command(run_command, action_spec["config"])
    run_args = shlex.split(run_command)

    # Special case handling for the `cohortextractor generate_cohort` command
    if is_generate_cohort_command(run_args, require_version=1):
        # Set the size of the dummy data population, if that's what we're
        # generating.  Possibly this should be moved to the study definition
        # anyway, which would make this unnecessary.
        if using_dummy_data_backend:
            if "dummy_data_file" in action_spec:
                run_command += f" --dummy-data-file={action_spec['dummy_data_file']}"
            else:
                size = int(project["expectations"]["population_size"])
                run_command += f" --expectations-population={size}"
        # Automatically configure the cohortextractor to produce output in the
        # directory the `outputs` spec is expecting. Longer term I'd like to
        # just make it an error if the directories don't match, rather than
        # silently fixing it. (We can use the project versioning system to
        # ensure this doesn't break existing studies.)
        output_dirs = get_output_dirs(action_spec["outputs"])
        if len(output_dirs) != 1:
            # If we detect multiple output directories but the command
            # explicitly specifies an output directory then we assume the user
            # knows what they're doing and don't attempt to modify the output
            # directory or throw an error
            if not args_include(run_args, "--output-dir"):
                raise ProjectValidationError(
                    f"generate_cohort command should produce output in only one "
                    f"directory, found {len(output_dirs)}:\n"
                    + "\n".join([f" - {d}/" for d in output_dirs])
                )
        else:
            run_command += f" --output-dir={output_dirs[0]}"

    elif is_generate_cohort_command(run_args, require_version=2):
        # cohortextractor Version 2 expects all command line arguments to be
        # specified in the run command
        if using_dummy_data_backend and "--dummy-data-file" not in run_command:
            raise ProjectValidationError(
                "--dummy-data-file is required for a local run"
            )

        # There is one and only one output file in the outputs spec (verified
        # in validate_project_and_set_defaults())
        output_files = [
            output_file
            for output in action_spec["outputs"].values()
            for output_file in output.values()
        ]
        output_file = next(iter(output_files))
        if output_file not in run_command:
            raise ProjectValidationError(
                "--output in run command and outputs must match"
            )

    # TODO: we can probably remove this fork since the v1&2 forks cover it
    elif is_generate_cohort_command(run_args):  # pragma: no cover
        raise RuntimeError("Unhandled cohortextractor version")

    return ActionSpecifiction(
        run=run_command,
        needs=action_spec.get("needs", []),
        outputs=action_spec["outputs"],
    )


def add_config_to_run_command(run_command, config):
    """Add --config flag to command.

    For commands that require complex config, users can supply a config key in
    project.yaml.  We serialize this as JSON, and pass it to the command with the
    --config flag.
    """
    config_as_json = json.dumps(config).replace("'", r"\u0027")
    return f"{run_command} --config '{config_as_json}'"


def is_generate_cohort_command(args, require_version=None):
    """
    The `cohortextractor generate_cohort` command gets special treatment in
    various places (e.g. it's the only command which gets access to the
    database) so it's helpful to have a single function for identifying it
    """
    assert not isinstance(args, str)
    version_found = None
    if len(args) > 1 and args[1] in ("generate_cohort", "generate_dataset"):
        if args[0].startswith("cohortextractor:"):
            version_found = 1
        # databuilder is a rebranded cohortextractor-v2.
        # Retain cohortextractor-v2 for backwards compatibility for now.
        elif args[0].startswith(("cohortextractor-v2:", "databuilder:")):
            version_found = 2

    # If we're not looking for a specific version then return True if any
    # version found
    if require_version is None:
        return version_found is not None
    # Otherwise return True only if specified version found
    else:
        return version_found == require_version


def args_include(args, target_arg):
    return any(arg == target_arg or arg.startswith(f"{target_arg}=") for arg in args)


def get_all_actions(project):
    # We ignore any manually defined run_all action (in later project versions
    # this will be an error). We use a list comprehension rather than set
    # operators as previously so we preserve the original order.
    return [action for action in project["actions"].keys() if action != RUN_ALL_COMMAND]


def get_all_output_patterns_from_project_file(project_file):
    project = parse_and_validate_project_file(project_file)
    all_patterns = set()
    for action in project["actions"].values():
        for patterns in action["outputs"].values():
            all_patterns.update(patterns.values())
    return list(all_patterns)


def get_output_dirs(output_spec):
    """
    Given the set of output files specified by an action, return a list of the
    unique directory names of those outputs
    """
    filenames = []
    for group in output_spec.values():
        filenames.extend(group.values())
    dirs = {PurePosixPath(filename).parent for filename in filenames}
    return list(dirs)
