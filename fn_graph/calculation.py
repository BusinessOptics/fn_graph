import sys
import traceback
from collections import Counter
from enum import Enum
from functools import reduce
from inspect import Parameter, signature
from logging import getLogger

import networkx as nx
from littleutils import ensure_list_if_string

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

    log.debug("Invalid nodes %s", invalid_nodes)
    log.debug("Retrived Nodes %s", must_be_retrieved)

    execution_order = list(nx.topological_sort(dag))
    execution_instructions = []
    for node in execution_order:
        log.debug(node)
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
    log.debug("Direct invalid nodes %s", direct_invalid_nodes)

    # If a node is invalidate all it's descendents must be made invalid
    indirect_invalid_nodes = (
        reduce(
            lambda a, b: a | b,
            [nx.descendants(dag, node) for node in direct_invalid_nodes] + [set()],
        )
    ) - direct_invalid_nodes
    log.debug("Indirect invalid nodes %s", indirect_invalid_nodes)

    for node in indirect_invalid_nodes:
        composer._cache.invalidate(composer, node)


def coalesce_argument_names(function, predecessor_results):
    sig = signature(function)

    positional_names = []
    args_name = None
    keyword_names = []
    kwargs_name = None

    for key, parameter in sig.parameters.items():
        if parameter.kind in (
            parameter.KEYWORD_ONLY,
            parameter.POSITIONAL_OR_KEYWORD,
            parameter.POSITIONAL_ONLY,
        ):
            if (
                parameter.default is not Parameter.empty
                and key not in predecessor_results
            ):
                continue

            if args_name is None:
                positional_names.append(key)
            else:
                keyword_names.append(key)
        elif parameter.kind == parameter.VAR_POSITIONAL:
            args_name = key
        elif parameter.kind == parameter.VAR_KEYWORD:
            kwargs_name = key

    return positional_names, args_name, keyword_names, kwargs_name


def coalesce_arguments(function, predecessor_results):
    positional_names, args_name, keyword_names, kwargs_name = coalesce_argument_names(
        function, predecessor_results
    )

    positional = [predecessor_results.pop(name) for name in positional_names]
    args = [
        predecessor_results.pop(name)
        for name in list(predecessor_results.keys())
        if name.startswith(args_name)
    ]
    keywords = {name: predecessor_results.pop(name) for name in keyword_names}
    kwargs = {
        name: predecessor_results.pop(name)
        for name in list(predecessor_results)
        if name.startswith(kwargs_name)
    }

    assert len(predecessor_results) == 0

    return positional, args, keywords, kwargs


def calculate_collect_exceptions(
    composer,
    outputs,
    perform_checks=True,
    intermediates=False,
    progress_callback=None,
    raise_immediately=False,
):
    """
    Executes the required parts of the function graph to product results
    for the given outputs.

    Args:
        composer: The composer to calculate
        outputs: list of the names of the functions to calculate
        perform_checks: if true error checks are performed before calculation
        intermediates: if true the results of all functions calculated will be returned
        progress_callback: a callback that is called as the calculation progresses,\
                this be of the form `callback(event_type, details)`

    Returns:
        Tuple: Tuple of (results, exception_info), results is a dictionary of results keyed by 
               function name, exception_info (etype, evalue, etraceback, node) is the information 
               about the exception if there was any.
    """
    outputs = ensure_list_if_string(outputs)

    progress_callback = progress_callback or (lambda *args, **kwargs: None)

    progress_callback("start_calculation", dict(outputs=outputs))

    if perform_checks:
        try:
            for name in outputs:
                if name not in composer._functions:
                    raise Exception(
                        f"'{name}' is not a composed function in this {composer.__class__.__name__} object."
                    )

            for error in composer.check(outputs):
                raise Exception(error)
        except:
            if raise_immediately:
                raise
            else:
                etype, evalue, etraceback = sys.exc_info()

                return {}, (etype, evalue, etraceback, None)

    maintain_cache_consistency(composer)

    # Limit to only the functions we care about
    dag = composer.ancestor_dag(outputs)

    if intermediates:
        outputs = dag.nodes()

    execution_instructions = get_execution_instructions(composer, dag, outputs)
    log.debug(execution_instructions)

    # Results store
    results = {}

    # Number of time a functions results still needs to be accessed
    remaining_usage_counts = Counter(pred for pred, _ in dag.edges())
    progress_callback(
        "prepared_calculation",
        dict(execution_instructions=execution_instructions, execution_graph=dag),
    )
    log.debug("Starting execution")
    for node, instruction in execution_instructions:
        progress_callback(
            "start_step", dict(name=node, execution_instruction=instruction)
        )
        try:
            predecessors = list(composer._resolve_predecessors(node))

            if instruction == NodeInstruction.IGNORE:
                log.debug("Ignoring function '%s'", node)

            elif instruction == NodeInstruction.RETRIEVE:
                log.debug("Retrieving function '%s'", node)
                try:
                    progress_callback("start_cache_retrieval", dict(name=node))
                    results[node] = composer._cache.get(composer, node)
                finally:
                    progress_callback("end_cache_retrieval", dict(name=node))
            else:
                log.debug("Calculating function '%s'", node)
                function = composer._functions[node]

                # Pull up arguments
                predecessor_results = {
                    parameter: results[pred] for parameter, pred in predecessors
                }
                positional, args, keywords, kwargs = coalesce_arguments(
                    function, predecessor_results
                )

                try:
                    progress_callback("start_function", dict(name=node))
                    result = function(*positional, *args, **keywords, **kwargs)
                except Exception as e:
                    if raise_immediately:
                        raise
                    else:
                        etype, evalue, etraceback = sys.exc_info()

                        return results, (etype, evalue, etraceback, node)
                finally:
                    progress_callback("end_function", dict(name=node))

                results[node] = result

                try:
                    progress_callback("start_cache_store", dict(name=node))
                    composer._cache.set(composer, node, result)
                finally:
                    progress_callback("end_cache_store", dict(name=node))

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
        finally:
            progress_callback(
                "end_step",
                dict(
                    name=node,
                    execution_instruction=instruction,
                    result=results.get(node),
                ),
            )

    # We should just be left with the results
    return results, None


def calculate(*args, **kwargs):
    """
    Executes the required parts of the function graph to product results
    for the given outputs.

    Args:
        composer: The composer to calculate
        outputs: list of the names of the functions to calculate
        perform_checks: if true error checks are performed before calculation
        intermediates: if true the results of all functions calculated will be returned
        progress_callback: a callback that is called as the calculation progresses,\
                this be of the form `callback(event_type, details)`

    Returns:
        Dictionary: Dictionary of results keyed by function name
    """
    results, _ = calculate_collect_exceptions(*args, raise_immediately=True, **kwargs)
    return results
