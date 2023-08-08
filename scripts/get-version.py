#!/usr/bin/env python
import re
import sys
from datetime import date


version_pat = re.compile(r"(?P<year>\d{4}).(?P<month>\d{2}).(?P<counter>\d)")


class UnexpectedVersion(Exception):
    pass


class VersionParseError(Exception):
    pass


def main(version_path=None):
    if version_path is None:
        version_path = "version"

    with open(version_path) as f:
        version_string = f.read()

    match = version_pat.match(version_string)
    if not match:
        raise VersionParseError()

    # extract values from current version
    current_counter = int(match.group("counter"))
    current_date = date(
        int(match.group("year")),
        int(match.group("month")),
        1,
    )

    # normalise dates to the first day since we don't care about days
    today_date = date.today().replace(day=1)

    if current_date > today_date:
        # didn't expect to find a version in the future
        raise UnexpectedVersion()

    # only bump the counter if the date has changed
    if today_date == current_date:
        counter = current_counter + 1
    else:
        counter = 1

    # we know that today's date is >= current_date at this point so we can rely
    # on using it for the version
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)

    print(f"{year}.{month}.{counter}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        version_path = sys.argv[1]
    else:
        version_path = None

    sys.exit(main(version_path))
