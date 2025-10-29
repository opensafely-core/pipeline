# list available commands
default:
    @"{{ just_executable() }}" --list


# clean up temporary files
clean:
    rm -rf .venv

# Install production requirements into and remove extraneous packages from venv
prodenv:
    uv sync --no-dev


# && dependencies are run after the recipe has run. Needs just>=0.9.9. This is
# a killer feature over Makefiles.
#
# Install dev requirements into venv without removing extraneous packages
devenv: && install-precommit
    uv sync --inexact


# Ensure precommit is installed
install-precommit:
    #!/usr/bin/env bash
    set -euo pipefail

    BASE_DIR=$(git rev-parse --show-toplevel)
    test -f $BASE_DIR/.git/hooks/pre-commit || uv run pre-commit install


# Upgrade a single package to the latest version as of the cutoff in pyproject.toml
upgrade-package package: && devenv
    uv lock --upgrade-package {{ package }}


# Upgrade all packages to the latest versions as of the cutoff in pyproject.toml
upgrade-all: && devenv
    uv lock --upgrade


# Move the cutoff date in pyproject.toml to N days ago (default: 7) at midnight UTC
bump-uv-cutoff days="7":
    #!/usr/bin/env -S uvx --with tomlkit python3

    import datetime
    import tomlkit

    with open("pyproject.toml", "rb") as f:
        content = tomlkit.load(f)

    new_datetime = (
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=int("{{ days }}"))
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    new_timestamp = new_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    if existing_timestamp := content["tool"]["uv"].get("exclude-newer"):
        if new_datetime < datetime.datetime.fromisoformat(existing_timestamp):
            print(
                f"Existing cutoff {existing_timestamp} is more recent than {new_timestamp}, not updating."
            )
            exit(0)
    content["tool"]["uv"]["exclude-newer"] = new_timestamp

    with open("pyproject.toml", "w") as f:
        tomlkit.dump(content, f)


# This is the default input command to update-dependencies action
# https://github.com/bennettoxford/update-dependencies-action
# Bump the timestamp cutoff to midnight UTC 7 days ago and upgrade all dependencies
update-dependencies: bump-uv-cutoff upgrade-all

update-fastparser-dependencies:
    uv pip compile --upgrade fastparser/requirements.in -o fastparser/requirements.txt

build-fastparser-wheel:
    #!/usr/bin/env bash
    rm -rf fastparser/dist
    uv build --wheel fastparser
    mv fastparser/dist/*.whl .


# install fast YAML parsing library
install-fastparser: devenv
    uv pip install --only-binary ':all:' file:///$PWD/opensafely_fastparser-1.0-py3-none-any.whl


# *args is variadic, 0 or more. This allows us to do `just test -k match`, for example.
# Run the tests
test python-version="$(cat .python-version)" extra="" *args="":
    uv run --python {{ python-version }} {{ extra }} coverage run --module pytest
    uv run --python {{ python-version }} {{ extra }} coverage report || $BIN/coverage html


test-with-fastparser python-version="$(cat .python-version)":
    just test {{ python-version }} "--extra fastparser"


format python-version="$(cat .python-version)" *args="--diff --quiet .":
    uv run --python {{ python-version }} ruff format --check {{ args }}


lint python-version="$(cat .python-version)" *args="--output-format=full .":
    uv run --python {{ python-version }} ruff check {{ args }}


mypy python-version="$(cat .python-version)":
    uv run --python {{ python-version }} mypy


# Runs the various dev checks but does not change any files
check python-version="$(cat .python-version)": && (format python-version) (lint python-version) (mypy python-version) # Check the lockfile before `uv run` is used
    #!/usr/bin/env bash
    set -euo pipefail

    # Make sure dates in pyproject.toml and uv.lock are in sync
    unset UV_EXCLUDE_NEWER
    rc=0
    uv lock --check --python {{ python-version }} || rc=$?
    if test "$rc" != "0" ; then
        echo "Timestamp cutoffs in uv.lock must match those in pyproject.toml. See DEVELOPERS.md for details and hints." >&2
        exit $rc
    fi


# Fix formatting and import sort ordering
fix:
    uv run ruff check --fix .
    uv run ruff format .


# Run the dev project
run: devenv
    echo "Not implemented yet"


package-build:
    rm -rf dist
    uv build


package-test type python-version="$(cat .python-version)": package-build
    #!/usr/bin/env bash
    VENV="test-{{ type }}"
    distribution_suffix="{{ if type == "wheel" { "whl" } else { "tar.gz" } }}"

    # build a fresh venv
    # We're using uv to create the venv here so we can use it for our
    # pythons, but we deliberately activate the venc and pip install
    # the distribution so we can check it's working in the non-uv way
    uv venv --python {{ python-version }} --seed $VENV
    . $VENV/bin/activate

    # clean up after ourselves, even if there are errors
    trap 'rm -rf $VENV' EXIT

    # ensure a modern pip
    $VENV/bin/pip install pip --upgrade

    # install the wheel distribution
    $VENV/bin/pip install dist/*."$distribution_suffix"

    # Minimal check that it has actually built correctly
    $VENV/bin/python -c "import pipeline"

    # check we haven't packaged tests with it
    unzip -Z -1 dist/*.whl | grep -vq "^tests/"


# Cut a release of this package
release:
    #!/usr/bin/env bash
    set -euo pipefail

    CALVER=$(date -u +"%Y.%m.%d.%H%M%S")

    git checkout main
    git pull
    git checkout -b release-$CALVER
    echo $CALVER > version
    git add version
    git commit --message "Release $CALVER"
    gh pr create --fill
