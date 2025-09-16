import pytest
from harmony_service_lib.exceptions import HarmonyException

def test_harmony_exception_str_representation():
    """Test that HarmonyException properly calls parent __init__ so str() works correctly"""
    message = "Test error message"

    with pytest.raises(HarmonyException, match='.*Test error message$'):
        raise HarmonyException(message)


def test_harmony_exception_with_custom_exception():
    """Test that CustomExceptions with params expecte properly."""
    class CustomError(HarmonyException):
        def __init__(self, params: set[str]):
            message = f'Params are bad: {params}'
            super().__init__(message)

    with pytest.raises(CustomError, match=r'Params are bad\:.*'):
        raise CustomError({'bad', 'param'})
