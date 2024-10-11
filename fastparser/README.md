We optionally support installing a fast YAML parser to make parsing very
large YAML files happen in a more reasonable time.

We don't want downstream consumers to have to manually specify the
parsing library but instead we want to allow them to say "give me
whatever library it is that `pipeline` wants".

The requirements file here specifies the targeted parsing library. This
can be installed in one of two ways:

1. Applications which depend directly on the `pipeline` library can use
   the `fastparser` "extras" specification:
   ```
   opensafely-pipeline[fastparser] @ git+https://github.com/opensafely-core/pipeline@vXXXX.XX.XX
   ```

2. Users who have installed `opensafely-cli` locally can install the
   pre-built wheel directly from Github (this wheel contains no code
   itself but declares dependencies on the relevant libraries):
   ```
   pip install https://github.com/opensafely-core/pipeline/raw/main/opensafely_fastparser-1.0-py3-none-any.whl
   ```

The `test_fastparser.py` test ensures that these stay in sync.
