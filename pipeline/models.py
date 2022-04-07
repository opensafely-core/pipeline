import shlex
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set

from pydantic import BaseModel, root_validator, validator

from .features import LATEST_VERSION


class Expectations(BaseModel):
    population_size: int = 1000


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

    @validator("run", pre=True)
    def parse_run_string(cls, run: str) -> Command:
        parts = shlex.split(run)
        name, _, version = parts[0].partition(":")
        args = " ".join(parts[1:])
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

    @validator("actions")
    def validate_unique_commands(cls, actions: Dict[str, Action]) -> Dict[str, Action]:
        seen: Dict[Command, List[str]] = defaultdict(list)
        for name, action in actions.items():
            run = action.run
            if run in seen:
                raise ValueError(
                    f"Action {name} has the same 'run' command as other actions: {' ,'.join(seen[run])}"
                )
            seen[run].append(name)

        return actions

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
