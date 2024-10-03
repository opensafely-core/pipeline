import pytest

from pipeline.features import get_feature_flags_for_version


def test_get_feature_flags_for_version_with_v1():
    flags = get_feature_flags_for_version(1)

    assert not flags.UNIQUE_OUTPUT_PATH
    assert not flags.EXPECTATIONS_POPULATION
    assert not flags.REMOVE_SUPPORT_FOR_COHORT_EXTRACTOR


def test_get_feature_flags_for_version_with_v2():
    flags = get_feature_flags_for_version(2)

    assert flags.UNIQUE_OUTPUT_PATH
    assert not flags.EXPECTATIONS_POPULATION
    assert not flags.REMOVE_SUPPORT_FOR_COHORT_EXTRACTOR


def test_get_feature_flags_for_version_with_v3():
    flags = get_feature_flags_for_version(3)

    assert flags.UNIQUE_OUTPUT_PATH
    assert flags.EXPECTATIONS_POPULATION
    assert not flags.REMOVE_SUPPORT_FOR_COHORT_EXTRACTOR


def test_get_feature_flags_for_version_with_v4():
    flags = get_feature_flags_for_version(4)

    assert flags.UNIQUE_OUTPUT_PATH
    assert not flags.EXPECTATIONS_POPULATION
    assert flags.REMOVE_SUPPORT_FOR_COHORT_EXTRACTOR


def test_get_feature_flags_for_version_with_v5():
    msg = "The latest version is v4, but got v5"
    with pytest.raises(ValueError, match=msg):
        get_feature_flags_for_version(5)
