"""
Toy pipeline — tests the full orchestration layer without needing
QXR mono repo access or real Docker/Lambda.

Four dummy nodes: load_data -> preprocess -> run_model -> postprocess

Run (all in memory):
    python toy_pipeline.py --config config/toy_local.yaml

Run (model node in docker, rest in memory):
    python toy_pipeline.py --config config/toy_docker.yaml
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from fn_graph import Composer

# ── Node functions ────────────────────────────────────────────────────────────
# These are "interior functions" — we never touch them.
# They receive kwargs matching upstream node names.

def load_data(raw_path: str) -> list:
    print(f"    [load_data] reading from {raw_path}")
    return [{"id": 1, "value": 10}, {"id": 2, "value": 20}]


def preprocess(load_data: list) -> list:
    print(f"    [preprocess] normalising {len(load_data)} records")
    return [{"id": r["id"], "value": r["value"] / 100.0} for r in load_data]


def run_model(preprocess: list) -> list:
    print(f"    [run_model] scoring {len(preprocess)} samples")
    return [{"id": r["id"], "score": round(r["value"] * 0.9, 4)} for r in preprocess]


def postprocess(run_model: list) -> dict:
    print(f"    [postprocess] filtering results")
    results = [s for s in run_model if s["score"] > 0.05]
    return {"results": results, "count": len(results)}


# ── Build fn_graph Composer ───────────────────────────────────────────────────
# fn_graph infers the DAG from function signatures — no explicit wiring needed.

f = (
    Composer()
    .update(
        load_data=load_data,
        preprocess=preprocess,
        run_model=run_model,
        postprocess=postprocess,
    )
    .update_parameters(raw_path="/data/input.csv")
)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from config import load_config, get_artifact_store, get_node_config
    from composer import PipelineComposer

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/toy_local.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    on_failure = config["pipeline"].get("on_failure", "stop")
    artifact_store = get_artifact_store(config)

    all_nodes = list(f.dag().nodes())
    execution_config = {node: get_node_config(config, node) for node in all_nodes}

    print("\nTopological order:", list(f.dag().nodes()))
    print("Executors:", {n: c.get("executor") for n, c in execution_config.items()})

    pipeline = PipelineComposer(
        execution_config=execution_config,
        artifact_store=artifact_store,
        on_failure=on_failure,
    )
    results = pipeline.run(f)

    print("\n── Final outputs ──")
    for name, value in results.items():
        print(f"  {name}: {value}")
