import pytest
from ruamel.yaml.error import YAMLStreamError

from pipeline import YAMLError
from pipeline.loading import parse_yaml_file


# def test_parse_yaml_file_with_bad_indentation(bad_indentation):
#     with pytest.raises(YAMLError) as exc_info:
#         parse_yaml_file(bad_indentation, filename="test.yaml")

#     original_exception = exc_info.value.__cause__

#     assert original_exception.note is None
#     assert original_exception.context_mark is None
#     assert original_exception.problem_mark.name == bad_indentation.name


def test_parse_yaml_file_with_duplicate_key(duplicate_key):
    with pytest.raises(YAMLError) as exc_info:
        parse_yaml_file(duplicate_key.read(), filename="test.yaml")

    original_exception = exc_info.value.__cause__

    assert original_exception.note is None
    assert original_exception.context_mark.name == "test.yaml"
    assert original_exception.problem_mark.name == "test.yaml"


def test_parse_yaml_file_with_duplicate_key_and_no_filename(duplicate_key):
    with pytest.raises(YAMLError) as exc_info:
        parse_yaml_file(duplicate_key.read())

    original_exception = exc_info.value.__cause__

    assert original_exception.note is None
    assert original_exception.context_mark.name == "<unicode string>"
    assert original_exception.problem_mark.name == "<unicode string>"


def test_parse_yaml_file_with_no_yaml():
    with pytest.raises(YAMLError) as exc_info:
        parse_yaml_file([])

    assert isinstance(exc_info.value.__cause__, YAMLStreamError)
