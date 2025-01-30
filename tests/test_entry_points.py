import pytest

from awesome_api.entry_points import make_executable


def function_to_test(a: str, b: str):
    return a + b


def test_make_executable_with_good_params():
    assert make_executable(custom_args=["a=1", "b=1"])(function_to_test)() == "11"


def test_make_executable_with_bad_params():
    with pytest.raises(SyntaxError):
        _ = make_executable(custom_args=["1", "1"])(function_to_test)()
