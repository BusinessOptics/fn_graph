import base64
import traceback as tb_module

import cloudpickle
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI()

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


class ExecuteRequest(BaseModel):
    node_name: str
    fn_source: str
    kwargs_b64: str


@app.get("/health")
def health():
    print("[worker] health check received", flush=True)
    return {"status": "ok"}


@app.post("/execute")
def execute(req: ExecuteRequest):
    try:
        print(f"[worker] received request for node: {req.node_name}", flush=True)
        print(f"[worker] fn_source length: {len(req.fn_source)} chars", flush=True)

        namespace = dict(_BASE_NAMESPACE)
        exec(req.fn_source, namespace)
        fn = namespace[req.node_name]
        print(f"[worker] function loaded: {req.node_name}", flush=True)

        kwargs = cloudpickle.loads(base64.b64decode(req.kwargs_b64))
        print(f"[worker] inputs loaded: {list(kwargs.keys())}", flush=True)

        print(f"[worker] running {req.node_name}...", flush=True)
        result = fn(**kwargs)
        print(f"[worker] {req.node_name} complete, output type: {type(result).__name__}", flush=True)

        result_b64 = base64.b64encode(cloudpickle.dumps(result, protocol=4)).decode()
        return {"result_b64": result_b64}
    except Exception as e:
        tb = tb_module.format_exc()
        print(f"[worker] ERROR in node '{req.node_name}': {e}", flush=True)
        print(f"[worker] traceback:\n{tb}", flush=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": tb},
        )
