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
devenv:
    uv sync --inexact

# Upgrade a single package
upgrade-package package: && uvmirror devenv
    uv lock --upgrade-package {{ package }}

# Upgrade all packages to the latest versions (with cooldown)
upgrade-all cooldown="7 days ago": && uvmirror devenv _build-fastparser-if-required
    just update-fastparser-dependencies "{{ cooldown }}"
    uv lock --upgrade --exclude-newer "{{ cooldown }}"

# update the uv mirror requirements file
uvmirror file="requirements.uvmirror":
    rm -f {{ file }}
    uv export --format requirements-txt --frozen --no-hashes --all-groups --all-extras > {{ file }}

# This is the default input command to update-dependencies action
# https://github.com/bennettoxford/update-dependencies-action

update-dependencies: upgrade-all

_build-fastparser-if-required:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! git diff --quiet -- fastparser/requirements.txt; then
        just build-fastparser-wheel
    fi

update-fastparser-dependencies cooldown="7 days ago":
    uv pip compile --exclude-newer="{{ cooldown }}" --upgrade fastparser/requirements.in -o fastparser/requirements.txt

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

format python-version="$(cat .python-version)" args="":
    uv run --python {{ python-version }} ruff format --diff --quiet {{ args }} .

lint python-version="$(cat .python-version)" args="":
    uv run --python {{ python-version }} ruff check --output-format=full {{ args }} .

mypy python-version="$(cat .python-version)":
    uv run --python {{ python-version }} mypy

lint-actions:
    docker run --rm -v $(pwd):/repo:ro --workdir /repo rhysd/actionlint:1.7.8 -color

# Run the various dev checks but does not change any files
check python-version="$(cat .python-version)":
    #!/usr/bin/env bash
    set -euo pipefail

    failed=0

    check() {
      echo -e "\e[1m=> ${1}\e[0m"
      rc=0
      # Run it
      eval $1 || rc=$?
      # Increment the counter on failure
      if [[ $rc != 0 ]]; then
        failed=$((failed + 1))
        # Add spacing to separate the error output from the next check
        echo -e "\n"
      fi
    }

    check "just check-lockfile"
    check "just format {{ python-version }}"
    check "just lint {{ python-version }}"
    check "just mypy {{ python-version }}"
    check "just lint-actions"
    test -d docker/ && check "just docker/lint"

    if [[ $failed > 0 ]]; then
      echo -en "\e[1;31m"
      echo "   $failed checks failed"
      echo -e "\e[0m"
      exit 1
    fi

# validate uv.lock
check-lockfile:
    uv lock --check

# Fix formatting, import sort ordering, and justfile
fix:
    uv run ruff check --fix .
    uv run ruff format .
    just --fmt --unstable

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
