from enum import Enum
from functools import reduce
import networkx as nx
from littleutils import ensure_list_if_string
from collections import Counter
from logging import getLogger
import time

from littleutils import select_keys

log = getLogger(__name__)


class NodeInstruction(Enum):
    CALCULATE = 1
    RETRIEVE = 2
    IGNORE = 3


def get_execution_instructions(composer, dag, outputs):

    direct_invalid_nodes = {
        node for node in dag if not composer._cache.valid(composer, node)
    }

    invalid_nodes = (
        reduce(
            lambda a, b: a | b,
            [nx.descendants(dag, node) for node in direct_invalid_nodes] + [set()],
        )
        | direct_invalid_nodes
    )

    must_be_retrieved = (
        {
            node
            for node in dag.nodes()
            if any(succ in invalid_nodes for succ in dag.successors(node))
        }
        | set(outputs)
    ) - invalid_nodes

    execution_order = list(nx.topological_sort(dag))
    execution_instructions = []
    for node in execution_order:
        if node in invalid_nodes:
            node_instruction = NodeInstruction.CALCULATE
        elif node in must_be_retrieved:
            node_instruction = NodeInstruction.RETRIEVE
        else:
            node_instruction = NodeInstruction.IGNORE
        execution_instructions.append((node, node_instruction))

    return execution_instructions


def maintain_cache_consistency(composer):
    dag = composer.dag()

    direct_invalid_nodes = {
        node for node in dag if not composer._cache.valid(composer, node)
    }

    # If a node is invalidate all it's descendents must be made invalid
    indirect_invalid_nodes = (
        reduce(
            lambda a, b: a | b,
            [nx.descendants(dag, node) for node in direct_invalid_nodes] + [set()],
        )
    ) - direct_invalid_nodes

    for node in indirect_invalid_nodes:
        composer._cache.invalidate(composer, node)


def calculate(
    composer, outputs, perform_checks=True, intermediates=False, progress_callback=None
):
    """
    Executes the required parts of the function graph to product results
    for the given outputs.

    Args:
        outputs: list of the names of the functions to calculate
        perform_checks: if true error checks are performed before calculation
        intermediates: if true the results of all functions calculated will be returned
        progress_callback: a callback that is called as the calculation progresses,\
                this be of the form `callback(event_type, details)`

    Returns:
        Dictionary: Dictionary of results keyed by function name
    """
    outputs = ensure_list_if_string(outputs)

    if perform_checks:
        for name in outputs:
            if name not in composer._functions:
                raise Exception(
                    f"'{name}' is not a composed function in this {composer.__class__.__name__} object."
                )

        for error in composer.check():
            raise Exception(error)

    progress_callback = progress_callback or (lambda *args, **kwargs: None)

    maintain_cache_consistency(composer)

    # Limit to only the functions we care about
    dag = composer.ancestor_dag(outputs)

    if intermediates:
        outputs = dag.nodes()

    execution_instructions = get_execution_instructions(composer, dag, outputs)

    progress_callback(
        "prepare_execution",
        dict(execution_instructions=execution_instructions, execution_graph=dag),
    )

    # Results store
    results = {}

    # Number of time a functions results still needs to be accessed
    remaining_usage_counts = Counter(pred for pred, _ in dag.edges())

    log.debug("Starting execution")
    for node, instruction in execution_instructions:
        start = time.time()
        progress_callback(
            "start_function", dict(name=name, execution_instruction=instruction)
        )
        predecessors = list(composer._resolve_predecessors(node))

        if instruction == NodeInstruction.IGNORE:
            log.debug("Ignoring function '%s'", node)

        elif instruction == NodeInstruction.RETRIEVE:
            log.debug("Retrieving function '%s'", node)
            results[node] = composer._cache.get(composer, node)
        else:
            log.debug("Calculating function '%s'", node)
            fn = composer._functions[node]

            # Pull up arguments
            arguments = {parameter: results[pred] for parameter, pred in predecessors}
            result = fn(**arguments)
            results[node] = result
            composer._cache.set(composer, node, result)

        # Eject results from memory once the are not needed
        remaining_usage_counts.subtract([pred for _, pred in predecessors])
        ready_to_eject = [
            key
            for key, value in remaining_usage_counts.items()
            if value == 0 and key not in outputs
        ]
        for key in ready_to_eject:
            assert key not in outputs
            remaining_usage_counts.pop(key)
            results.pop(key, "not_found")

        period = time.time() - start

        progress_callback(
            "end_function",
            dict(
                name=name,
                execution_instruction=instruction,
                time=period,
                result=results.get(node),
            ),
        )

    # We should just be left with the results
    return results
    return select_keys(results, outputs)
