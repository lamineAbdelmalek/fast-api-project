import sys
from typing import Callable, Optional, Sequence


def make_executable(
    custom_args: Optional[Sequence[str]] = None,
) -> Callable:
    def inner(python_function_with_string_args: Callable) -> Callable:
        def wrapper():
            function_args = dict()
            cli_args = custom_args if custom_args else sys.argv[1:]
            for arg in cli_args:
                key_value_pair = arg.split("=", 1)
                if len(key_value_pair) != 2:
                    function_name = python_function_with_string_args.__name__
                    raise SyntaxError(
                        f"You must enter parameters and values separated by '='."
                        f" See example below :\n"
                        f"{function_name} arg1=value1 arg2=value2"
                    )
                arg_name, arg_value = key_value_pair
                function_args[arg_name] = arg_value
            return python_function_with_string_args(**function_args)

        return wrapper

    return inner


@make_executable()
def simple_task(param1: str, param2: str):
    print(f"this is param1 : {param1} and this is param2 : {param2}")
