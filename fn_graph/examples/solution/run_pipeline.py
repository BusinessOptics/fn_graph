"""
Entry point for the pluggable pipeline orchestration layer.

Usage:
    python run_pipeline.py --pipeline fn_graph.examples.machine_learning --config config/iris.yaml
    python run_pipeline.py --pipeline fn_graph.examples.finance --config config/finance.yaml
"""

import argparse
import importlib
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from config import load_config, get_artifact_store, get_node_config
from composer import PipelineComposer


class _Tee:
    """Duplicates writes to both stdout and a log file."""
    def __init__(self, real, log_file):
        self._real = real
        self._log = log_file

    def write(self, data):
        self._real.write(data)
        self._log.write(data)

    def flush(self):
        self._real.flush()
        self._log.flush()

    def fileno(self):
        return self._real.fileno()


def _setup_log(pipeline: str, run_id: str) -> Path:
    pipeline_name = pipeline.split(".")[-1]
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir = Path(__file__).parent / "logs" / pipeline_name / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{timestamp}.log"
    return log_path


def main():
    print("[run_pipeline] starting", flush=True)

    parser = argparse.ArgumentParser(description="Run an fn_graph pipeline.")
    parser.add_argument("--pipeline", required=True, help="Dotted module path, e.g. fn_graph.examples.machine_learning")
    parser.add_argument("--config", required=True, help="Path to config yaml, e.g. config/iris.yaml")
    args = parser.parse_args()

    print(f"[run_pipeline] pipeline: {args.pipeline}", flush=True)
    print(f"[run_pipeline] config:   {args.config}", flush=True)

    module = importlib.import_module(args.pipeline)
    f = module.f

    # Pandas >= 2.0 compatibility patch for finance pipeline
    if hasattr(module, "cumulative_return"):
        import inspect as _inspect
        try:
            _src = _inspect.getsource(module.cumulative_return)
        except OSError:
            _src = ""
        if "total_position[-1]" in _src:
            def cumulative_return(total_position):
                return 100 * (total_position.iloc[-1] / total_position.iloc[0] - 1)
            f = f.update(cumulative_return=cumulative_return)
            print("[run_pipeline] applied pandas compatibility patch: cumulative_return", flush=True)

    config = load_config(args.config)
    run_id = config["pipeline"]["run_id"]
    on_failure = config["pipeline"].get("on_failure", "stop")

    # Logging setup
    log_path = _setup_log(args.pipeline, run_id)
    log_file = open(log_path, "w", encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, log_file)
    print(f"[run_pipeline] logging to: {log_path}", flush=True)

    artifact_store = get_artifact_store(config)
    print(f"[run_pipeline] artifact store: {type(artifact_store).__name__}", flush=True)

    # Build per-node execution config for every node in the DAG
    all_nodes = list(f.dag().nodes())
    execution_config = {node: get_node_config(config, node) for node in all_nodes}

    print("[run_pipeline] executors configured:", flush=True)
    for node_name, node_cfg in execution_config.items():
        print(f"  {node_name}: {node_cfg.get('executor', 'memory')}", flush=True)

    pipeline = PipelineComposer(
        execution_config=execution_config,
        artifact_store=artifact_store,
        on_failure=on_failure,
    )
    results = pipeline.run(f)

    print("\n" + "=" * 60, flush=True)
    print("=== Pipeline Results ===", flush=True)
    import matplotlib.figure
    for name, value in results.items():
        if isinstance(value, str):
            print(f"\n[{name}]\n{value}", flush=True)
        elif isinstance(value, (int, float)):
            print(f"[{name}] {value}", flush=True)
        elif isinstance(value, matplotlib.figure.Figure):
            out_path = Path(args.config).parent / f"{name}.png"
            value.savefig(out_path)
            print(f"[{name}] figure saved to {out_path}", flush=True)
        elif hasattr(value, "get_figure"):
            out_path = Path(args.config).parent / f"{name}.png"
            value.get_figure().savefig(out_path)
            print(f"[{name}] figure saved to {out_path}", flush=True)
        else:
            print(f"[{name}] {type(value).__name__}", flush=True)

    print(f"\n[run_pipeline] log saved to: {log_path}", flush=True)
    log_file.close()
    sys.stdout = sys.__stdout__


if __name__ == "__main__":
    main()
