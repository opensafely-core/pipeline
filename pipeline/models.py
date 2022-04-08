import json
import shlex
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set

from pydantic import BaseModel, root_validator, validator

from .exceptions import InvalidPatternError
from .extractors import is_extraction_command
from .features import LATEST_VERSION, get_feature_flags_for_version
from .validation import assert_valid_glob_pattern


class Expectations(BaseModel):
    population_size: int

    @validator("population_size", pre=True)
    def validate_population_size(cls, population_size) -> int:
        try:
            return int(population_size)
        except (TypeError, ValueError):
            raise ValueError(
                "Project expectations population size must be a number",
            )


class Outputs(BaseModel):
    highly_sensitive: Optional[Dict[str, str]]
    moderately_sensitive: Optional[Dict[str, str]]
    minimally_sensitive: Optional[Dict[str, str]]

    @root_validator()
    def at_least_one_output(cls, outputs: Dict[str, str]) -> Dict[str, str]:
        if not any(outputs.values()):
            raise ValueError(
                f"must specify at least one output of: {', '.join(outputs)}"
            )

        return outputs

    @root_validator(pre=True)
    def validate_output_filenames_are_valid(
        cls, outputs: Dict[str, str]
    ) -> Dict[str, str]:
        # we use pre=True here so that we only get the outputs specified in the
        # input data.  With Optional[…] wrapped fields pydantic will set None
        # for us and that just makes the logic a little fiddler with no
        # benefit.
        for privacy_level, output in outputs.items():
            for output_id, filename in output.items():
                try:
                    assert_valid_glob_pattern(filename)
                except InvalidPatternError as e:
                    raise ValueError(f"Output path {filename} is not permitted: {e}")

        return outputs


class Command(BaseModel):
    run: str  # original string
    name: str
    version: str
    args: str

    class Config:
        # this makes Command hashable, which for some reason due to the
        # Action.parse_run_string works, pydantic requires.
        frozen = True


class Action(BaseModel):
    config: Optional[Dict[Any, Any]] = None
    run: Command
    needs: List[str] = []
    outputs: Outputs

    @root_validator(pre=True)
    def add_config_to_run(cls, values):
        """
        Add --config flag to command.

        For commands that require complex config, users can supply a config key
        in project.yaml.  We serialize this as JSON, and pass it to the command
        with the --config flag.

        We run this with pre=True so the raw input's run key is mutated before
        parse_run_string runs.
        """

        if "config" not in values:
            return values

        # For commands that require complex config, users can supply a
        # config key in project.yaml.  We serialize this as JSON, and pass
        # it to the command with the --config flag.
        config_as_json = json.dumps(values["config"]).replace("'", r"\u0027")
        values["run"] = f"{values['run']} --config '{config_as_json}'"

        return values

    @validator("run", pre=True)
    def parse_run_string(cls, run: str) -> Command:
        parts = shlex.split(run)
        name, _, version = parts[0].partition(":")
        args = " ".join(parts[1:])

        if not version:
            raise ValueError(
                f"{name} must have a version specified (e.g. {name}:0.5.2)",
            )

        return Command(
            run=run,
            name=name,
            version=version,
            args=args,
        )


class Pipeline(BaseModel):
    version: float
    expectations: Expectations
    actions: Dict[str, Action]

    @root_validator(pre=True)
    def validate_expectations_per_version(cls, values):
        """Ensure the expectations key exists for version 3 onwards"""
        try:
            version = float(values.get("version"))
        except (TypeError, ValueError):
            # this is handled in the validate_version_exists and
            # validate_version_value validators
            return values

        feat = get_feature_flags_for_version(version)

        if not feat.EXPECTATIONS_POPULATION:
            # set the default here because pydantic doesn't seem to set it
            # otherwise
            values["expectations"] = {"population_size": 1000}
            return values

        if "expectations" not in values:
            raise ValueError("Project must include `expectations` section")

        if "population_size" not in values["expectations"]:
            raise ValueError(
                "Project `expectations` section must include `population_size` section",
            )

        return values

    @root_validator(pre=True)
    def validate_extraction_command_has_only_one_output(cls, values):
        for action_id, config in values["actions"].items():
            args = shlex.split(config["run"])
            num_outputs = len(config["outputs"])
            if is_extraction_command(args) and num_outputs != 1:
                raise ValueError(
                    "A `generate_cohort` action must have exactly one output; "
                    f"{action_id} had {num_outputs}"
                )

        return values

    @root_validator()
    def validate_outputs_per_version(cls, values):
        """
        Ensure outputs are unique for version 2 onwards

        We validate this on Pipeline so we can get the version
        """

        # we're not using pre=True in the validator so we can rely on the
        # version and action keys being the correct type but we have to handle
        # them not existing
        if not (version := values.get("version")):
            return values  # handle missing version

        if (actions := values.get("actions")) is None:
            return values  # hand no actions

        feat = get_feature_flags_for_version(version)
        if not feat.UNIQUE_OUTPUT_PATH:
            return values

        # find duplicate paths defined in the outputs section
        seen_files = []
        for config in actions.values():
            for output in config.outputs.dict(exclude_unset=True).values():
                for filename in output.values():
                    if filename in seen_files:
                        raise ValueError(f"Output path {filename} is not unique")

                    seen_files.append(filename)

        return values

    @validator("actions")
    def validate_unique_commands(cls, actions: Dict[str, Action]) -> Dict[str, Action]:
        seen: Dict[Command, List[str]] = defaultdict(list)
        for name, config in actions.items():
            run = config.run
            if run in seen:
                raise ValueError(
                    f"Action {name} has the same 'run' command as other actions: {' ,'.join(seen[run])}"
                )
            seen[run].append(name)

        return actions

    @validator("actions")
    def validate_needs_are_comma_delimited(
        cls, actions: Dict[str, Action]
    ) -> Dict[str, Action]:
        space_delimited = {}
        for name, action in actions.items():
            # find needs definitions with spaces in them
            incorrect = [dep for dep in action.needs if " " in dep]
            if incorrect:
                space_delimited[name] = incorrect

        if not space_delimited:
            return actions

        def iter_incorrect_needs(space_delimited):
            for name, needs in space_delimited.items():
                yield f"Action: {name}"
                for need in needs:
                    yield f" - {need}"

        msg = [
            "`needs` actions should be separated with commas. The following actions need fixing:",
            *iter_incorrect_needs(space_delimited),
        ]

        raise ValueError("\n".join(msg))

    @validator("actions")
    def validate_needs_exist(cls, actions: Dict[str, Action]) -> Dict[str, Action]:
        missing = {}
        for name, action in actions.items():
            unknown_needs = set(action.needs) - set(actions)
            if unknown_needs:
                missing[name] = unknown_needs

        if not missing:
            return actions

        def iter_missing_needs(missing: Dict[str, Set[str]]) -> Iterable[str]:
            for name, needs in missing.items():
                yield f"Action: {name}"
                for need in needs:
                    yield f" - {need}"

        msg = [
            "One or more actions is referencing unknown actions in its needs list:",
            *iter_missing_needs(missing),
        ]
        raise ValueError("\n".join(msg))

    @root_validator(pre=True)
    def validate_version_exists(cls, values):
        """
        Ensure the version key exists.

        This is a re-implementation of pydantic's field validation so we can
        get a custom error message.  This can be removed when we add a wrapper
        around the models to generate more UI friendly error messages.
        """
        if "version" in values:
            return values

        raise ValueError(
            f"Project file must have a `version` attribute specifying which "
            f"version of the project configuration format it uses (current "
            f"latest version is {LATEST_VERSION})"
        )

    @validator("version", pre=True)
    def validate_version_value(cls, value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"`version` must be a number between 1 and {LATEST_VERSION}"
            )
