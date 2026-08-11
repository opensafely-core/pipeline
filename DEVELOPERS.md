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
Commands for adding, removing or modifying constraints of dependencies will apply a 7 day cooldown by default.

Changes to dependencies should be made via `uv` commands, or by modifying `pyproject.toml` directly followed by
[locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/) via `uv` or `just` commands like
`just devenv` or `just upgrade-all`. You should not modify `uv.lock` manually.

Note that `uv.lock` must be reproducible from `pyproject.toml`. Otherwise, `just check` will fail.

If you require package versions that are newer than the default cooldown, pass an
alternative cooldown, using any format accepted by ruff's `exclude-newer` e.g.:

```
just upgrade-all "3 days ago"
```

Note that automated tooling that runs with the defaults will override any non-default cooldowns and upgrade/downgrade package versions accordingly.


### Adding a package-specific timestamp cutoff

It is possible to specify a package-specific timestamp cutoff.
This should be done in the `pyproject.toml` to ensure reproducible installs;
see the [uv documentation](https://docs.astral.sh/uv/reference/settings/#exclude-newer-package) for details.

If set, the package-specific cutoff will take precedence over the default global cutoff regardless of which one is more recent.

You should not set a package-specific cutoff in order to pin an older version - use a version constraint instead.
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
