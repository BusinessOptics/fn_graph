import base64
import inspect
import json
from typing import Any, Callable

import boto3
import cloudpickle

from .base import BaseExecutor

_LAMBDA_PAYLOAD_LIMIT = 6 * 1024 * 1024  # 6 MB


class LambdaExecutor(BaseExecutor):
    def __init__(self, function_name: str, region: str):
        self.function_name = function_name
        self.region = region

    def execute(self, node_name: str, fn: Callable, kwargs: dict) -> Any:
        print(f"[LambdaExecutor] invoking Lambda for node: {node_name}", flush=True)
        print(f"[LambdaExecutor] function: {self.function_name}, region: {self.region}", flush=True)

        fn_source = inspect.getsource(fn)
        kwargs_b64 = base64.b64encode(cloudpickle.dumps(kwargs, protocol=4)).decode()
        payload = json.dumps({"node_name": node_name, "fn_source": fn_source, "kwargs_b64": kwargs_b64})
        payload_bytes = payload.encode()
        size_mb = len(payload_bytes) / (1024 * 1024)

        if len(payload_bytes) > _LAMBDA_PAYLOAD_LIMIT:
            print(f"[LambdaExecutor] ERROR: payload {size_mb:.2f}MB exceeds Lambda 6MB limit", flush=True)
            raise ValueError(
                f"Payload for node '{node_name}' is {size_mb:.2f}MB, which exceeds the 6MB Lambda "
                "limit. Switch to an S3 artifact store and pass S3 paths instead."
            )

        print(f"[LambdaExecutor] payload size: {size_mb:.2f}MB, invoking...", flush=True)

        client = boto3.client("lambda", region_name=self.region)
        response = client.invoke(
            FunctionName=self.function_name,
            InvocationType="RequestResponse",
            Payload=payload_bytes,
        )
        print(f"[LambdaExecutor] Lambda response received", flush=True)

        raw = response["Payload"].read()
        result_payload = json.loads(raw)

        status_code = result_payload.get("statusCode", 200)
        if status_code == 500 or "error" in result_payload:
            print(f"[LambdaExecutor] ERROR in node '{node_name}': {result_payload.get('error')}", flush=True)
            print(f"[LambdaExecutor] traceback:\n{result_payload.get('traceback', '')}", flush=True)
            raise RuntimeError(f"Lambda error in node '{node_name}': {result_payload.get('error')}")

        result = cloudpickle.loads(base64.b64decode(result_payload["result_b64"]))
        print(f"[LambdaExecutor] node {node_name} complete, output type: {type(result).__name__}", flush=True)
        return result
