import re
import shlex
from collections import defaultdict

from .constants import RUN_ALL_COMMAND
from .exceptions import InvalidPatternError, ValidationError
from .features import LATEST_VERSION, get_feature_flags_for_version
from .validation import (
    assert_valid_glob_pattern,
    validate_cohortextractor_outputs,
    validate_databuilder_outputs,
)


cohortextractor_pat = re.compile(r"cohortextractor:\S+ generate_cohort")
databuilder_pat = re.compile(r"databuilder|ehrql:\S+ generate[-_]dataset")

# orderd by most common, going forwards
DB_COMMANDS = {
    "ehrql": ("generate-dataset", "generate-measures"),
    "sqlrunner": "*",  # all commands are valid
    "cohortextractor": ("generate_cohort", "generate_codelist_report"),
    "databuilder": ("generate-dataset",),
}


def is_database_action(args):
    """
    By default actions do not have database access, but certain trusted actions require it
    """
    image = args[0]
    image = image.split(":")[0]
    db_commands = DB_COMMANDS.get(image)
    if db_commands is None:
        return False

    if db_commands == "*":
        return True

    # no command specified
    if len(args) == 1:
        return False

    # 1st arg is command
    return args[1] in db_commands


class Expectations:
    def __init__(self, population_size):
        try:
            self.population_size = int(population_size)
        except (TypeError, ValueError):
            raise ValidationError(
                "Project expectations population size must be a number",
            )


class Outputs:
    def __init__(
        self,
        highly_sensitive=None,
        moderately_sensitive=None,
        minimally_sensitive=None,
        **kwargs,
    ):
        self.highly_sensitive = highly_sensitive
        self.moderately_sensitive = moderately_sensitive
        self.minimally_sensitive = minimally_sensitive

        self.at_least_one_output()
        self.validate_output_filenames_are_valid()

    def __len__(self):
        return len(self.dict())

    def dict(self):
        d = {
            k: getattr(self, k)
            for k in [
                "highly_sensitive",
                "moderately_sensitive",
                "minimally_sensitive",
            ]
        }
        return {k: v for k, v in d.items() if v is not None}

    def at_least_one_output(self):
        if not self.dict():
            raise ValidationError(
                f"must specify at least one output of: {', '.join(vars(self))}"
            )

    def validate_output_filenames_are_valid(self):
        for privacy_level, output in self.dict().items():
            for output_id, filename in output.items():
                try:
                    assert_valid_glob_pattern(filename, privacy_level)
                except InvalidPatternError as e:
                    raise ValidationError(f"Output path {filename} is invalid: {e}")


class Command:
    def __init__(self, raw):
        self.raw = raw

    def __eq__(self, other):
        if not isinstance(other, Command):  # pragma: no cover
            return NotImplemented
        return self.raw == other.raw

    def __hash__(self):
        return hash(self.raw)

    @property
    def args(self):
        return " ".join(self.parts[1:])

    @property
    def name(self):
        # parts[0] with version split off
        return self.parts[0].split(":")[0]

    @property
    def parts(self):
        return shlex.split(self.raw)

    @property
    def version(self):
        # parts[0] with name split off
        return self.parts[0].split(":")[1]


class Action:
    def __init__(self, outputs, run, needs=None, config=None, dummy_data_file=None):
        self.outputs = Outputs(**outputs)
        self.run = self.parse_run_string(run)
        self.needs = needs or []
        self.config = config
        self.dummy_data_file = dummy_data_file

    def parse_run_string(self, run):
        parts = shlex.split(run)

        name, _, version = parts[0].partition(":")
        if not version:
            raise ValidationError(
                f"{name} must have a version specified (e.g. {name}:0.5.2)",
            )

        return Command(raw=run)

    @property
    def is_database_action(self):
        return is_database_action(self.run.parts)


class Pipeline:
    def __init__(self, version=None, actions=None, expectations=None):
        self.validate_version_exists(version)
        self.version = self.validate_version_value(version)

        self.validate_actions_run(actions)
        self.actions = {
            action_id: Action(**action_config)
            for action_id, action_config in actions.items()
        }
        self.validate_actions()
        self.validate_needs_are_comma_delimited()
        self.validate_needs_exist()
        self.validate_unique_commands()
        self.validate_outputs_per_version()

        expectations = self.validate_expectations_per_version(expectations)
        self.expectations = Expectations(**expectations)

    @property
    def all_actions(self):
        """
        Get all actions for this Pipeline instance

        We ignore any manually defined run_all action (in later project
        versions this will be an error). We use a list comprehension rather
        than set operators as previously so we preserve the original order.
        """
        return [action for action in self.actions.keys() if action != RUN_ALL_COMMAND]

    def validate_actions(self):
        # TODO: move to Action when we move name onto it
        validators = {
            cohortextractor_pat: validate_cohortextractor_outputs,
            databuilder_pat: validate_databuilder_outputs,
        }
        for action_id, config in self.actions.items():
            for cmd, validator_func in validators.items():
                if cmd.match(config.run.raw):
                    validator_func(action_id, config)

    def validate_expectations_per_version(self, expectations):
        """Ensure the expectations key exists for version 3 onwards"""
        feat = get_feature_flags_for_version(self.version)

        if not feat.EXPECTATIONS_POPULATION:
            return {"population_size": 1000}

        if expectations is None:
            raise ValidationError("Project must include `expectations` section")

        if "population_size" not in expectations:
            raise ValidationError(
                "Project `expectations` section must include `population_size` section",
            )

        return expectations

    def validate_outputs_per_version(self):
        """
        Ensure outputs are unique for version 2 onwards

        We validate this on Pipeline so we can get the version
        """

        feat = get_feature_flags_for_version(self.version)
        if not feat.UNIQUE_OUTPUT_PATH:
            return

        # find duplicate paths defined in the outputs section
        seen_files = []
        for config in self.actions.values():
            for output in config.outputs.dict().values():
                for filename in output.values():
                    if filename in seen_files:
                        raise ValidationError(f"Output path {filename} is not unique")

                    seen_files.append(filename)

    def validate_actions_run(self, actions):
        # TODO: move to Action when we move name onto it
        for action_id, config in actions.items():
            if config["run"] == "":
                # key is present but empty
                raise ValidationError(
                    f"run must have a value, {action_id} has an empty run key"
                )

    def validate_unique_commands(self):
        seen = defaultdict(list)
        for name, config in self.actions.items():
            run = config.run
            if run in seen:
                raise ValidationError(
                    f"Action {name} has the same 'run' command as other actions: {' ,'.join(seen[run])}"
                )
            seen[run].append(name)

    def validate_needs_are_comma_delimited(self):
        space_delimited = {}
        for name, action in self.actions.items():
            # find needs definitions with spaces in them
            incorrect = [dep for dep in action.needs if " " in dep]
            if incorrect:
                space_delimited[name] = incorrect

        if not space_delimited:
            return

        def iter_incorrect_needs(space_delimited):
            for name, needs in space_delimited.items():
                yield f"Action: {name}"
                for need in needs:
                    yield f" - {need}"

        msg = [
            "`needs` actions should be separated with commas. The following actions need fixing:",
            *iter_incorrect_needs(space_delimited),
        ]

        raise ValidationError("\n".join(msg))

    def validate_needs_exist(self):
        missing = {}
        for name, action in self.actions.items():
            unknown_needs = set(action.needs) - set(self.actions)
            if unknown_needs:
                missing[name] = unknown_needs

        if not missing:
            return

        def iter_missing_needs(missing):
            for name, needs in missing.items():
                yield f"Action: {name}"
                for need in needs:
                    yield f" - {need}"

        msg = [
            "One or more actions is referencing unknown actions in its needs list:",
            *iter_missing_needs(missing),
        ]
        raise ValidationError("\n".join(msg))

    def validate_version_exists(self, version):
        """
        Ensure the version key exists.
        """
        if version is not None:
            return

        raise ValidationError(
            f"Project file must have a `version` attribute specifying which "
            f"version of the project configuration format it uses (current "
            f"latest version is {LATEST_VERSION})"
        )

    def validate_version_value(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValidationError(
                f"`version` must be a number between 1 and {LATEST_VERSION}"
            )
