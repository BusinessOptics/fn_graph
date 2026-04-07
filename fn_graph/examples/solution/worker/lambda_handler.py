"""
Lambda handler — runs inside the Lambda container (ECR image).
Receives the same payload as the Docker worker's /execute endpoint,
executes the node function, and returns the result.
"""
import base64
import json
import traceback as tb_module

import cloudpickle

# Pre-populate namespace same as Docker worker so node functions work unchanged
_BASE_NAMESPACE: dict = {}
for _mod_name, _alias in [
    ("sklearn", "sklearn"),
    ("sklearn.datasets", "sklearn.datasets"),
    ("sklearn.svm", "sklearn.svm"),
    ("sklearn.linear_model", "sklearn.linear_model"),
    ("sklearn.metrics", "sklearn.metrics"),
    ("sklearn.preprocessing", "sklearn.preprocessing"),
    ("sklearn.model_selection", "sklearn.model_selection"),
    ("pandas", "pd"),
    ("numpy", "np"),
    ("seaborn", "sns"),
    ("matplotlib.pylab", "plt"),
    ("matplotlib", "matplotlib"),
    ("networkx", "nx"),
]:
    try:
        import importlib as _il
        _mod = _il.import_module(_mod_name)
        _BASE_NAMESPACE[_alias] = _mod
        _BASE_NAMESPACE[_mod_name.split(".")[0]] = _il.import_module(_mod_name.split(".")[0])
    except ImportError:
        pass

try:
    from sklearn.model_selection import train_test_split as _tts
    _BASE_NAMESPACE["train_test_split"] = _tts
except ImportError:
    pass


def handler(event, context):
    """
    AWS Lambda entry point.

    Expected event keys:
        node_name   str   name of the node function to run
        fn_source   str   source code of the function (inspect.getsource)
        kwargs_b64  str   base64(cloudpickle(kwargs dict))

    Returns:
        { result_b64: str }  on success
        { statusCode: 500, error: str, traceback: str }  on failure
    """
    node_name = event.get("node_name", "<unknown>")
    print(f"[lambda_handler] received node: {node_name}", flush=True)

    try:
        fn_source = event["fn_source"]
        kwargs_b64 = event["kwargs_b64"]

        namespace = dict(_BASE_NAMESPACE)
        exec(fn_source, namespace)
        fn = namespace[node_name]
        print(f"[lambda_handler] function loaded: {node_name}", flush=True)

        kwargs = cloudpickle.loads(base64.b64decode(kwargs_b64))
        print(f"[lambda_handler] inputs: {list(kwargs.keys())}", flush=True)

        result = fn(**kwargs)
        print(f"[lambda_handler] {node_name} complete, output type: {type(result).__name__}", flush=True)

        result_b64 = base64.b64encode(cloudpickle.dumps(result, protocol=4)).decode()
        return {"result_b64": result_b64}

    except Exception as e:
        tb = tb_module.format_exc()
        print(f"[lambda_handler] ERROR in node '{node_name}': {e}", flush=True)
        print(f"[lambda_handler] traceback:\n{tb}", flush=True)
        return {"statusCode": 500, "error": str(e), "traceback": tb}
