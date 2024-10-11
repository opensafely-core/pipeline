import re
import zipfile


# We want to ensure that we don't forget to re-compile the wheel if we update the
# requirements
def test_fastparser_requirements_match_wheel():
    wheel_deps = set()
    with zipfile.ZipFile("opensafely_fastparser-1.0-py3-none-any.whl") as wheel:
        metadata_name = [
            name for name in wheel.namelist() if name.endswith("/METADATA")
        ][0]
        for line in wheel.open(metadata_name):
            if match := re.match(r"^Requires-Dist: (.*)$", line.decode("utf8")):
                wheel_deps.add(match.group(1).strip())

    requirements = set()
    with open("fastparser/requirements.txt") as f:
        for line in f:
            if match := re.match(r"^([^#]\S+)", line):
                requirements.add(match.group(1).strip())

    assert wheel_deps == requirements, "Run `just build-fastparser-wheel`"
