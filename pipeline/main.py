from __future__ import annotations

from .loading import parse_yaml_file
from .models import Pipeline


def load_pipeline(pipeline_config: str, filename: str | None = None) -> Pipeline:
    """
    Main entrypoint for function for parsing pipeline configs

    The given pipeline_config should be the contents of a config file to be
    parsed and validated.

    The optional filename will add filenames to validation errors of YAML
    configs, which is useful in user facing contexts.
    """
    # parse
    parsed_data = parse_yaml_file(pipeline_config, filename=filename)

    # validate
    return Pipeline(**parsed_data)
