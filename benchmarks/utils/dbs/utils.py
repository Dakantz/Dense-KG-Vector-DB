from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar


In = TypeVar("In")
Out = TypeVar("Out")


def run_with_timeout(operation: Callable[[In], Out], timeout=5) -> Out:
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(operation)
        try:
            result = future.result(timeout=timeout)
            return result
        except TimeoutError as te:
            raise te
