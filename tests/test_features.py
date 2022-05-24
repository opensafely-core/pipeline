from pipeline.features import get_feature_flags_for_version


def test_get_feature_flags_for_version_with_v1():
    flags = get_feature_flags_for_version(1)

    assert not flags.UNIQUE_OUTPUT_PATH
    assert not flags.EXPECTATIONS_POPULATION


def test_get_feature_flags_for_version_with_v2():
    flags = get_feature_flags_for_version(2)

    assert flags.UNIQUE_OUTPUT_PATH
    assert not flags.EXPECTATIONS_POPULATION


def test_get_feature_flags_for_version_with_v3():
    flags = get_feature_flags_for_version(3)

    assert flags.UNIQUE_OUTPUT_PATH
    assert flags.EXPECTATIONS_POPULATION
