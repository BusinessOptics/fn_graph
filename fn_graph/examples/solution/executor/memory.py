from typing import Any, Callable

from .base import BaseExecutor


class InMemoryExecutor(BaseExecutor):
    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        print(f"[InMemoryExecutor] running node: {node_name} directly in process", flush=True)
        print(f"[InMemoryExecutor] inputs: {list(kwargs.keys())}", flush=True)
        result = fn(**kwargs)
        print(f"[InMemoryExecutor] node {node_name} complete, output type: {type(result).__name__}", flush=True)
        return result
