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

### Overview
See the [uv documentation](https://docs.astral.sh/uv/concepts/projects/dependencies) for details on usage.
Commands for adding, removing or modifying constraints of dependencies will automatically respect the
global timestamp cutoff specified in the `pyproject.toml`:
```toml
[tool.uv]
exclude-newer = "YYYY-MM-DDTHH:MM:SSZ"
```
Changes to dependencies should be made via `uv` commands, or by modifying `pyproject.toml` directly followed by
[locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/) via `uv` or `just` commands like
`just devenv` or `just upgrade-all`. You should not modify `uv.lock` manually.

Note that `uv.lock` must be reproducible from `pyproject.toml`. Otherwise, `just check` will fail.
If `just check` errors saying that the timestamps must match, you might have modified one file but not the other:
  - If you modified `pyproject.toml`, you must update `uv.lock` via `uv lock` / `just upgrade-all` or similar.
  - If you did not modify `pyproject.toml` but have changes in `uv.lock`, you should revert the changes to `uv.lock`,
  modify `pyproject.toml` as you require, then run `uv lock` to update `uv.lock`.

The timestamp cutoff should usually be set to midnight UTC of a past date.
In general, the date is expected to be between 7 and 14 days ago as a result of automated weekly dependency updates.

If you require a package version that is newer than the cutoff allows, you can either manually bump the global cutoff
date or add a package-specific timestamp cutoff. Both options are described below.

### Manually bumping the cutoff date
The cutoff timestamp can be modified to a more recent date either manually in the `pyproject.toml`
or with `just bump-uv-cutoff <days-ago>`.
For example, to set the cutoff to today's date and upgrade all dependencies, run:
```
just bump-uv-cutoff 0
just upgrade-all
```

### Adding a package-specific timestamp cutoff
It is possible to specify a package-specific timestamp cutoff in addition to the global cutoff.
This should be done in the `pyproject.toml` to ensure reproducible installs;
see the [uv documentation](https://docs.astral.sh/uv/reference/settings/#exclude-newer-package) for details.
If set, the package-specific cutoff will take precedence over the global cutoff regardless of which one is more recent.

You should not set a package-specific cutoff that is older than the global cutoff - use a version
constraint instead.
If there is good reason to set a package-specific cutoff that is more recent than the global cutoff,
**care should be taken to ensure that the package-specific cutoff is manually removed once it is over 7 days old**,
as otherwise future automated updates of that package will be indefinitely blocked.
Currently no automated tooling is in place to enforce removal of stale package-specific cutoffs.


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
