# Notes for developers

## System requirements

### just

Follow installation instructions from the [Just Programmer's Manual](https://just.systems/man/en/packages.html "Follow installation instructions for your OS").

#### Add completion for your shell. E.g. for bash:
```
source <(just --completions bash)
```

#### Show all available commands
```
just #  shortcut for just --list
```

### uv

Follow installation instructions from the [uv documentation](https://docs.astral.sh/uv/getting-started/installation/) for your OS.


## Dependency management
Dependencies are managed with `uv`.
See the [uv documentation](https://docs.astral.sh/uv/concepts/projects/dependencies) for details on usage.

Changes to dependencies should be made via `uv` commands, or by modifying `pyproject.toml` directly followed by
[locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/) via `uv` or `just` commands like
`just devenv` or `just upgrade-all`. You should not modify `uv.lock` manually.

Note that `uv.lock` must be reproducible from `pyproject.toml`. Otherwise, `just check` will fail.
If `just check` errors, you might have modified one file but not the other:
  - If you modified `pyproject.toml`, you must update `uv.lock` via `uv lock` / `just upgrade-all` or similar.
  - If you did not modify `pyproject.toml` but have changes in `uv.lock`, you should revert the changes to `uv.lock`,
  modify `pyproject.toml` as you require, then run `uv lock` to update `uv.lock`.

## Local development environment


Set up a local development environment with:
```
just devenv
```

## Tests
Run the tests with:
```
just test <args>
```

This will use the python version specified in `.python-version`. Tests can be run with a specific python,
e.g. to run on python 3.11:
```
just test 3.11
```


## Releasing
To make a new release from the `main` branch:
```
just release
```
This will checkout the latest `main`, update the `version` file,
and create a new release PR for you.
