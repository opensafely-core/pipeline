# just has no idiom for setting a default value for an environment variable
# so we shell out, as we need VIRTUAL_ENV in the justfile environment
export VIRTUAL_ENV  := `echo ${VIRTUAL_ENV:-.venv}`

export BIN := VIRTUAL_ENV + "/bin"
export PIP := BIN + "/python -m pip"


# list available commands
default:
    @{{ just_executable() }} --list


# clean up temporary files
clean:
    rm -rf .venv


# ensure valid virtualenv
virtualenv:
    #!/usr/bin/env bash
    # allow users to specify python version in .env
    PYTHON_VERSION=${PYTHON_VERSION:-python3.8}

    # create venv and upgrade pip
    test -d $VIRTUAL_ENV || { $PYTHON_VERSION -m venv $VIRTUAL_ENV && $PIP install --upgrade pip; }

    # ensure we have pip-tools so we can run pip-compile
    test -e $BIN/pip-compile || $PIP install pip-tools


_compile src dst *args: virtualenv
    #!/usr/bin/env bash
    # exit if src file is older than dst file (-nt = 'newer than', but we negate with || to avoid error exit code)
    test "${FORCE:-}" = "true" -o {{ src }} -nt {{ dst }} || exit 0
    $BIN/pip-compile --allow-unsafe --output-file={{ dst }} {{ src }} {{ args }}


# update requirements.prod.txt if pyproject.toml has changed
requirements-prod *args:
    {{ just_executable() }} _compile pyproject.toml requirements.prod.txt {{ args }}


# update requirements.dev.txt if requirements.dev.in has changed
requirements-dev *args: requirements-prod
    {{ just_executable() }} _compile requirements.dev.in requirements.dev.txt {{ args }}


# ensure prod requirements installed and up to date
prodenv: virtualenv requirements-prod
    #!/usr/bin/env bash
    # exit if .txt file has not changed since we installed them (-nt == "newer than', but we negate with || to avoid error exit code)
    test requirements.prod.txt -nt $VIRTUAL_ENV/.prod || exit 0

    $PIP install -r requirements.prod.txt
    touch $VIRTUAL_ENV/.prod


# upgrade dev or prod dependencies (specify package to upgrade single package, all by default)
upgrade env package="": virtualenv
    #!/usr/bin/env bash
    opts="--upgrade"
    test -z "{{ package }}" || opts="--upgrade-package {{ package }}"
    FORCE=true {{ just_executable() }} requirements-{{ env }} $opts


# && dependencies are run after the recipe has run. Needs just>=0.9.9. This is
# a killer feature over Makefiles.
#
# ensure dev requirements installed and up to date
devenv: prodenv requirements-dev && install-precommit
    #!/usr/bin/env bash
    # exit if .txt file has not changed since we installed them (-nt == "newer than', but we negate with || to avoid error exit code)
    test requirements.dev.txt -nt $VIRTUAL_ENV/.dev || exit 0

    $PIP install -r requirements.dev.txt
    touch $VIRTUAL_ENV/.dev


build-fastparser-wheel: devenv
    #!/usr/bin/env bash
    rm -rf fastparser/dist
    $BIN/python -m build --wheel fastparser
    mv fastparser/dist/*.whl .


# install fast YAML parsing library
install-fastparser: devenv
    $PIP install --only-binary ':all:' file:///$PWD/opensafely_fastparser-1.0-py3-none-any.whl


# ensure precommit is installed
install-precommit:
    #!/usr/bin/env bash
    BASE_DIR=$(git rev-parse --show-toplevel)
    test -f $BASE_DIR/.git/hooks/pre-commit || $BIN/pre-commit install


# *args is variadic, 0 or more. This allows us to do `just test -k match`, for example.
# Run the tests
test *args: devenv
    $BIN/coverage run --module pytest {{ args }}
    $BIN/coverage report || $BIN/coverage html


package-build: virtualenv
    rm -rf dist

    $PIP install build
    $BIN/python -m build


package-test type: package-build
    #!/usr/bin/env bash
    VENV="test-{{ type }}"
    distribution_suffix="{{ if type == "wheel" { "whl" } else { "tar.gz" } }}"

    # build a fresh venv
    python -m venv $VENV

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


ruff *args=".": devenv
    $BIN/ruff format --diff --quiet .
    $BIN/ruff check --output-format=full .


mypy: devenv
    $BIN/mypy


# check format and linting
check: ruff mypy


# fix format and linting
fix: devenv
    $BIN/ruff format .
    $BIN/ruff check --fix .

# Run the dev project
run: devenv
    echo "Not implemented yet"


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
