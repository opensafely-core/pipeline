# OpenSAFELY Pipeline Parser

This library takes the contents of an OpenSAFELY pipeline configuration file (`project.yaml` or `pipeline.yaml`), validates it, and parses it into a typed structure.

For example:

    with open("/path/to/project.yaml") as f:
        data = load_pipeline(f.read())


The returned object is a Pydantic model, `Pipeline`, defined in `pipeline/models.py`.


## Developer docs

Please see the [additional information](DEVELOPERS.md).
