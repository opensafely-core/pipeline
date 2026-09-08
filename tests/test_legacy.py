from pipeline.legacy import get_all_output_patterns_from_project_file


def test_get_all_output_patterns_from_project_file_success(version):
    config = f"""
    version: {version}
    actions:
      first:
        run: python:v2 python analyse1.py
        outputs:
          moderately_sensitive:
            output: output/input.csv
            other: output/graph_*.png

      second:
        run: python:v2 python analyse2.py
        outputs:
          moderately_sensitive:
            second: output/*.csv
    """

    patterns = get_all_output_patterns_from_project_file(config)

    assert set(patterns) == {"output/input.csv", "output/graph_*.png", "output/*.csv"}
