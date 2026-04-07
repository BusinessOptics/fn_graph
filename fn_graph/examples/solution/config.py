import yaml

from executor.memory import InMemoryExecutor
from executor.docker import DockerExecutor
from executor.lambda_executor import LambdaExecutor
from artifact_store.fs import LocalFSArtifactStore
from artifact_store.s3 import S3ArtifactStore


def load_config(yaml_path: str) -> dict:
    print(f"[config] loading config from {yaml_path}", flush=True)
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    run_id = config["pipeline"]["run_id"]
    store_type = config["artifact_store"]["type"]
    nodes = config.get("nodes", {})
    print(f"[config] run_id: {run_id}", flush=True)
    print(f"[config] artifact store type: {store_type}", flush=True)
    print(f"[config] nodes configured: {list(nodes.keys())}", flush=True)
    return config


def get_executor(node_config: dict):
    """
    Build the right executor from a node config dict.
    Supports retry config on DockerExecutor.
    """
    if node_config is None:
        node_config = {"executor": "memory"}

    executor_type = node_config.get("executor", "memory")
    print(f"[config] creating executor: {executor_type}", flush=True)

    if executor_type == "memory":
        return InMemoryExecutor()

    elif executor_type == "docker":
        return DockerExecutor(
            image=node_config.get("image", "fn_graph_worker_v2"),
            retry=node_config.get("retry"),  # None = no retry = 1 attempt
        )

    elif executor_type == "lambda":
        return LambdaExecutor(
            function_name=node_config["function_name"],
            region=node_config["region"],
        )

    else:
        raise ValueError(f"Unknown executor type: '{executor_type}'")


def get_artifact_store(config: dict):
    store_cfg = config["artifact_store"]
    store_type = store_cfg["type"]
    run_id = config["pipeline"]["run_id"]
    print(f"[config] creating artifact store: {store_type}", flush=True)

    if store_type == "fs":
        return LocalFSArtifactStore(base_dir=store_cfg["base_dir"], run_id=run_id)

    elif store_type == "s3":
        return S3ArtifactStore(
            bucket=store_cfg["bucket"],
            run_id=run_id,
            region=store_cfg.get("region", "us-east-1"),
        )

    else:
        raise ValueError(f"Unknown artifact store type: '{store_type}'")


def get_node_config(config: dict, node_name: str) -> dict:
    """
    Returns the config for a node.
    Falls back to '*' wildcard, then to memory default.
    """
    nodes = config.get("nodes", {})
    return nodes.get(node_name) or nodes.get("*") or {"executor": "memory"}
