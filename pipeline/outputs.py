from pathlib import PurePosixPath


def get_output_dirs(output_spec):
    """
    Given the set of output files specified by an action, return a list of the
    unique directory names of those outputs
    """
    filenames = []
    for group in output_spec.values():
        filenames.extend(group.values())
    dirs = {PurePosixPath(filename).parent for filename in filenames}
    return list(dirs)
