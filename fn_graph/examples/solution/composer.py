"""
PipelineComposer — drop-in replacement for fn_graph.Composer.

The only change needed to use this instead of plain fn_graph:

    # Before
    from fn_graph import Composer

    # After (only this line changes)
    from fn_graph.examples.solution.composer import PipelineComposer as Composer

Everything else — update(), update_parameters(), calculate() — works identically.

What this adds on top of plain fn_graph:
- Each node runs through a pluggable executor (in-process, Docker, or Lambda)
- Each node's output is saved to an artifact store (local disk, S3, etc.)
- Re-runs skip nodes whose outputs already exist (memoization)
- If a node fails, downstream nodes are skipped automatically
- Retry with backoff on Docker nodes
- All of this is configured via a YAML file — no code changes needed
"""

import inspect
import networkx as nx
from fn_graph import Composer

from artifact_store.base import BaseArtifactStore
from config import get_executor, load_config, get_artifact_store, get_node_config


class PipelineComposer(Composer):
    """
    Subclass of fn_graph.Composer.

    Inherits update(), update_parameters(), update_from(), cache() unchanged.
    Only calculate() is overridden — it now routes each node through our
    executor and artifact store instead of running everything in one process.
    """

    def __init__(self, *, _functions=None, _parameters=None):
        super().__init__(_functions=_functions, _parameters=_parameters)

    # ── Ensure chained calls return PipelineComposer, not base Composer ───────

    def update(self, *args, **kwargs):
        base = super().update(*args, **kwargs)
        return PipelineComposer(_functions=base._functions, _parameters=base._parameters)

    def update_parameters(self, **kwargs):
        base = super().update_parameters(**kwargs)
        return PipelineComposer(_functions=base._functions, _parameters=base._parameters)

    def update_from(self, other):
        base = super().update_from(other)
        return PipelineComposer(_functions=base._functions, _parameters=base._parameters)

    # ── Graph helpers ─────────────────────────────────────────────────────────

    def dag(self):
        """
        Build the dependency graph from function signatures.
        fn_graph's implicit contract: if function B has a parameter named A,
        then B depends on A. We use this to build the execution order.
        """
        g = nx.DiGraph()
        all_names = set(self._functions) | set(self._parameters)
        for name in all_names:
            g.add_node(name)
        for fname, fn in self._functions.items():
            for param in inspect.signature(fn).parameters:
                if param in all_names:
                    g.add_edge(param, fname)
        return g

    def functions(self):
        """Returns all node functions registered in this composer."""
        return dict(self._functions)

    def parameters(self):
        """
        Returns pipeline-level parameters in the format:
        { name: (type, value) }
        These are the initial inputs seeded into the artifact store before
        any node runs.
        """
        return {k: (type(v), v) for k, v in self._parameters.items()}

    def _resolve_predecessors(self, node_name):
        """
        For a given node, yields (param_name, source_node) pairs.
        This tells the orchestrator exactly which upstream outputs to
        load from the artifact store as inputs for this node.
        """
        fn = self._functions.get(node_name)
        if fn is None:
            return
        all_names = set(self._functions) | set(self._parameters)
        for param in inspect.signature(fn).parameters:
            if param in all_names:
                yield (param, param)

    # ── calculate() — the orchestrator ───────────────────────────────────────

    def calculate(self, outputs, config="config/toy_local.yaml", **kwargs):
        """
        Override of fn_graph's calculate().

        This is the orchestrator — it:
        1. Reads the DAG and determines execution order (topological sort)
        2. Seeds pipeline parameters into the artifact store
        3. For each node in order:
           a. Checks if output already exists (memoization — skip if yes)
           b. Checks if any upstream node failed (skip if yes)
           c. Loads only the inputs that node needs from the artifact store
           d. Dispatches the node to the configured executor
              (in-process / Docker / Lambda — set in YAML config)
           e. Saves the output back to the artifact store
        4. Returns results for the requested output nodes

        Args:
            outputs:  list of node names you want results for,
                      or None / [] to return all node results
            config:   path to YAML config file, or a pre-loaded config dict

        Returns:
            dict of { node_name: result }
        """
        # Load config from file or use pre-loaded dict
        if isinstance(config, str):
            cfg = load_config(config)
        else:
            cfg = config

        on_failure = cfg["pipeline"].get("on_failure", "stop")
        artifact_store = get_artifact_store(cfg)

        dag = self.dag()
        funcs = self.functions()
        params = {name: val for name, (_, val) in self.parameters().items()}

        # Topological sort — nodes run in dependency order
        topo_order = list(nx.topological_sort(dag))
        total = len(topo_order)

        print(f"\n[PipelineComposer] starting pipeline run", flush=True)
        print(f"[PipelineComposer] execution order: {topo_order}", flush=True)
        print(f"[PipelineComposer] on_failure: {on_failure}", flush=True)

        # Step 1 — seed pipeline parameters into artifact store
        # These are the initial inputs (e.g. raw_path, model_type etc.)
        print("\n[PipelineComposer] seeding parameters", flush=True)
        for name, value in params.items():
            artifact_store.put(name, value)
            print(f"[PipelineComposer] seeded: {name} = {value}", flush=True)

        failed_nodes = set()

        print("\n[PipelineComposer] beginning node execution", flush=True)
        for i, node_name in enumerate(topo_order):
            print(f"\n[PipelineComposer] --- node: {node_name} ({i+1}/{total}) ---", flush=True)

            # Skip parameter nodes — they're already in the artifact store
            if node_name in params:
                print(f"[PipelineComposer] skipping parameter node: {node_name}", flush=True)
                continue

            # Memoization — if this node's output already exists, skip it
            # Change run_id in the YAML config to force a fresh run
            if artifact_store.exists(node_name):
                print(f"[PipelineComposer] output exists, skipping: {node_name}", flush=True)
                continue

            # If any upstream node failed, skip this node too
            deps = list(dag.predecessors(node_name))
            blocked_by = [d for d in deps if d in failed_nodes]
            if blocked_by:
                print(f"[PipelineComposer] skipping {node_name}: upstream failed: {blocked_by}", flush=True)
                failed_nodes.add(node_name)
                continue

            # Load only the inputs this node declared — not the full pipeline state
            # This minimises data transfer, especially important for Docker/Lambda
            predecessors = list(self._resolve_predecessors(node_name))
            input_names = [p for p, _ in predecessors]
            print(f"[PipelineComposer] loading inputs: {input_names}", flush=True)
            kwargs_node = {p: artifact_store.get(n) for p, n in predecessors}

            # Get the executor for this node (memory / docker / lambda)
            # Configured per-node in the YAML file
            node_config = get_node_config(cfg, node_name)
            executor = get_executor(node_config)
            print(f"[PipelineComposer] executor: {type(executor).__name__}", flush=True)

            try:
                # Run the node and save output to artifact store
                result = executor.execute(node_name, funcs[node_name], kwargs_node)
                artifact_store.put(node_name, result)
                print(f"[PipelineComposer] node {node_name} done", flush=True)

            except Exception as e:
                print(f"[PipelineComposer] ERROR in '{node_name}': {e}", flush=True)
                failed_nodes.add(node_name)
                if on_failure == "finish_running":
                    # Keep going — other independent nodes can still run
                    continue
                else:
                    # Stop the pipeline immediately
                    raise

        if failed_nodes:
            print(f"\n[PipelineComposer] finished with failures: {failed_nodes}", flush=True)
        else:
            print(f"\n[PipelineComposer] pipeline complete", flush=True)

        # Collect results from artifact store
        all_results = {
            name: artifact_store.get(name)
            for name in funcs
            if artifact_store.exists(name)
        }

        # Return only the requested outputs, or everything if none specified
        if outputs:
            return {k: all_results[k] for k in outputs if k in all_results}
        return all_results
