from typing import Literal

def foo(
    a: int,
    b: Literal["fail", "replace", "append"] = "fail",
        ):
    ...
