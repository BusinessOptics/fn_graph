from __future__ import annotations

import time
from collections import Counter, defaultdict, namedtuple
from functools import reduce
from inspect import Parameter, signature
from itertools import groupby
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, List, Dict

import graphviz
import joblib
import networkx as nx
from littleutils import (
    ensure_list_if_string,
    select_keys,
    strip_required_prefix,
    strip_required_suffix,
)

from .caches import DevelopmentCache, SimpleCache, NullCache
from .calculation import calculate
from fn_graph.calculation import NodeInstruction, get_execution_instructions

log = getLogger(__name__)


ComposerTestResult = namedtuple("TestResult", "name passed exception")
"""
The results of Composer.run_tests()
"""

# TODO cleverer use of caching - stop unnecessary reads


class Composer:
    """
    A function composer is responsible for orchestrating the composition
    and execution of graph of functions.

    Pure free functions are added to the composer, the names of the function
    arguments are used to determine how those functions are wired up into
    a directed acyclic graph.
    """

    def __init__(
        self,
        *,
        _functions=None,
        _parameters=None,
        _links=None,
        _cache=None,
        _tests=None,
    ):
        # These are namespaced
        self._functions = _functions or {}
        self._links = _links or {}
        self._tests = _tests or defaultdict(list)

        self._cache = _cache or NullCache()
        self._parameters = _parameters or {}

    def _copy(self, **kwargs):
        return type(self)(
            **{
                **dict(
                    _functions=self._functions,
                    _links=self._links,
                    _cache=self._cache,
                    _parameters=self._parameters,
                    _tests=self._tests,
                ),
                **kwargs,
            }
        )

    def update(self, *args: Callable, **kwargs: Callable) -> Composer:
        """
        Add functions to the composer.

        Args:
            args: Positional arguments use the __name__ of the function as the reference
        in the graph.
            kwargs: Keyword arguments use the key as the name of the function in the graph.
        
        Returns:
            A new composer with the functions added.
        """
        args_with_names = {arg.__name__: arg for arg in args}
        all_args = {**self._functions, **args_with_names, **kwargs}

        for argname, argument in all_args.items():
            if not callable(argument):
                raise Exception(
                    f"Argument '{argname}' is not a function or callable. All arguments must be callable."
                )

        return self._copy(_functions=all_args)

    def update_without_prefix(
        self, prefix: str, *functions: Callable, **kwargs: Callable
    ) -> Composer:
        """
        Given a prefix and a list of (named) functions, this adds the functions
        to the composer but first strips the prefix from their name. This is very
        useful to stop name shadowing.

        Args:
            prefix: The prefix to strip off the function names
            functions: functions to add while stripping the prefix
            kwargs: named functions to add
        Returns:
            A new composer with the functions added
        """

        args_with_names = {
            strip_required_prefix(arg.__name__, prefix): arg for arg in functions
        }
        return self.update(**args_with_names, **kwargs)

    def update_without_suffix(self, suffix, *functions, **kwargs) -> Composer:
        """
        Given a prefix and a list of (named) functions, this adds the functions
        to the composer but first strips the suffix from their name. This is very
        useful to stop name shadowing.

        Args:
            prefix: The suffix to strip off the function names
            functions: functions to add while stripping the suffix
            kwargs: named functions to add
        Returns:
            A new composer with the functions added
        """

        args_with_names = {
            strip_required_suffix(arg.__name__, suffix): arg for arg in functions
        }
        return self.update(**args_with_names, **kwargs)

    def update_from(self, *composers: Composer) -> Composer:
        """
        Create a new composer with all the functions from this composer
        as well as the the passed composers.
        
        Args:
            composers: The composers to take functions from

        Returns:
            A new Composer with all the input composers functions added.
        """
        return reduce(lambda x, y: x.update(**y._functions), [self, *composers])

    def update_namespaces(self, **namespaces: Composer) -> Composer:
        """
        Given a group of keyword named composers, create a series of functions
        namespaced by the keywords and drawn from the composers' functions.

        Args:
            namespaces: Composers that will be added at the namespace that corresponds \
            to the arguments key

        Returns:
            A new Composer with all the input composers functions added as namespaces.
        """
        return self._copy(
            **{
                arg: {
                    **getattr(self, arg),
                    **{
                        "__".join([namespace, k]): value
                        for namespace, composer in namespaces.items()
                        for k, value in getattr(composer, arg).items()
                    },
                }
                for arg in ["_functions", "_links", "_parameters"]
            }
        )

    def update_parameters(self, **parameters: Any) -> Composer:
        """
        Allows you to pass static parameters to the graph, they will be exposed as callables.
        """

        # Have to capture the value eagerly
        return self._copy(_parameters={**parameters, **self._parameters}).update(
            **{k: (lambda x: (lambda: x))(v) for k, v in parameters.items()}
        )

    def update_tests(self, **tests) -> Composer:
        """
        Adds tests to the composer. 
        
        A test is a function that should check a property of the calculations 
        results and raise an exception if they are not met.

        The work exactly the same way as functions in terms of resolution of 
        arguments. They are run with the run_test method.
        """
        return self._copy(_tests={**self._tests, **tests})

    def link(self, **kwargs):
        """
        Create a symlink between an argument name and a function output. 
        This is a convenience method. For example:

        `f.link(my_unknown_argument="my_real_function")`

        is the same as 

        `f.update(my_unknown_argument= lambda my_real_function: my_real_function)`

        """
        _links = {**self._links, **kwargs}
        return self._copy(_links=_links)

    def check(self):
        """
        Returns a generator of errors if there are any errors in the function graph.
        """
        dag = self.dag()
        cycles = list(nx.simple_cycles(dag))
        if cycles:
            yield dict(
                type="cycle",
                message=f"Cycle found [{', '.join(cycles[0])}]. The function graph must be acyclic.",
            )
        for unbound_fn, calling_fns in self._unbound().items():
            yield dict(
                type="unbound",
                message=f"Unbound function '{unbound_fn}' required.",
                function=unbound_fn,
                referers=calling_fns,
            )

    def calculate(
        self, outputs, perform_checks=True, intermediates=False, progress_callback=None
    ):
        return calculate(
            self, outputs, perform_checks, intermediates, progress_callback
        )

    def run_tests(self):
        """
        Run all the composer tests.

        Returns:
            A generator of ComposerTestResults(name, passed, exception).
        """

        all_referenced_functions = [
            self._resolve_predecessor(tname, pname)
            for tname, fn in self._tests.items()
            for pname in signature(fn).parameters
        ]

        results = self.calculate(all_referenced_functions)

        test_results = []
        for tname, fn in self._tests.items():

            arguments = {
                pname: results[self._resolve_predecessor(tname, pname)]
                for pname in signature(fn).parameters
            }

            try:
                fn(**arguments)
                test_result = ComposerTestResult(
                    name=tname, passed=True, exception=None
                )
            except Exception as e:
                test_result = ComposerTestResult(name=tname, passed=False, exception=e)

            yield test_result

    def call(self, output):
        """
        A convenience method to calculate a single output
        """
        return self.calculate([output])[output]

    def precalculate(self, outputs):
        """
        Create a new Composer where the results of the given functions have
        been pre-calculated.

        :param outputs: list of the names of the functions to pre-calculate
        :rtype: A composer
        """
        results = self.calculate(outputs)
        return self.update(
            **{k: (lambda x: (lambda: x))(v) for k, v in results.items()}
        )

    def __getattr__(self, name):
        """
        Allow composed functions to be easily called.
        """
        name = name.replace(".", "__")
        if name in self._functions:
            return lambda: self.calculate([name])[name]
        else:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute '{name}', nor any composed function '{name}'."
            )

    def raw_function(self, name):
        """
        Access a raw function in the composer by name.
        """
        return self._functions[name]

    def dag(self):
        """
        Generates the DAG representing the function graph.

        :rtype: a networkx.DiGraph instance with function names as nodes
        """

        G = nx.DiGraph()

        for key in self._functions:
            G.add_node(key)

        for key, fn in self._functions.items():
            predecessors = self._resolve_predecessors(key)
            G.add_edges_from([(resolved, key) for _, resolved in predecessors])

        return G

    def ancestor_dag(self, outputs):
        """
        A dag of all the ancestors of the given outputs, i.e. the functions that must be calculated 
        to for the given outputs.
        """
        full_dag = self.dag()
        ancestors = set(outputs) | {
            pred for output in outputs for pred in nx.ancestors(full_dag, output)
        }
        return full_dag.subgraph(ancestors)

    def subgraph(self, function_names):
        """
        Given a collection of function names this will create a new 
        composer that only consists of those nodes.
        """
        return self._copy(
            _functions={k: self._functions[k] for k in function_names},
            _parameters={
                k: v for k, v in self._parameters.items() if k in function_names
            },
            _links={k: v for k, v in self._links.items() if v in function_names},
        )

    def cache(self, backend=None) -> Composer:
        """
        Create a new composer with a given cache backend.

        By default this is a Simple cache.
        """
        backend = backend or SimpleCache()
        return self._copy(_cache=backend)

    def development_cache(self, name, cache_dir=None) -> Composer:
        """
        Create a new composer with a development cache setup
        """
        return self.cache(DevelopmentCache(name, cache_dir))

    def cache_clear(self):
        """
        Clear the cache
        """
        for key in self.dag():
            self._cache.invalidate(self, key)

    def cache_invalidate(self, *nodes: List[str]):
        """
        Invalidate the cache for all nodes affected (the descendants) by the 
        given nodes.
        """
        to_invalidate = set()
        for node in nodes:
            to_invalidate.update(nx.descendants(self.dag(), node))
            to_invalidate.add(node)

        for key in to_invalidate:
            self._cache.invalidate(self, key)

    def cache_graphviz(self, outputs=[]):
        """
        Display a graphviz with the cache invalidated nodes highlighted.
        """
        instructions = get_execution_instructions(self, self.dag(), outputs)

        filter = self.ancestor_dag(outputs).nodes() if outputs else None

        def get_node_styles(instruction):
            return {
                NodeInstruction.IGNORE: dict(fillcolor="green"),
                NodeInstruction.RETRIEVE: dict(fillcolor="orange"),
                NodeInstruction.CALCULATE: dict(fillcolor="red"),
            }[instruction]

        extra_node_styles = {
            node: get_node_styles(instruction) for node, instruction in instructions
        }
        return self.graphviz(extra_node_styles=extra_node_styles, filter=filter)

    def graphviz(
        self,
        *,
        hide_parameters=False,
        flatten=False,
        highlight=None,
        filter=None,
        extra_node_styles=None,
    ):
        """
        Generates a graphviz.DiGraph that is suitable for display.

        This requires graphviz to be installed.

        The output can be directly viewed in a Jupyter notebook.
        """
        extra_node_styles = extra_node_styles or {}
        highlight = highlight or []
        dag = self.dag()

        if filter is None:
            filter = dag.nodes()

        tree = self._build_name_tree()

        unbound = set(self._unbound().keys())

        # Recursively build from the tree
        def create_subgraph(tree, name=None):

            label = name
            if flatten:
                name = f"flat_{name}" if name else None
            else:
                name = f"cluster_{name}" if name else None

            g = graphviz.Digraph(name=name)
            if label:
                g.attr("graph", label=label, fontname="arial")

            for k, v in tree.items():
                if isinstance(v, str):
                    name = v
                    if hide_parameters and name in self._parameters:
                        continue

                    node_styles = dict(
                        style="rounded, filled", fontname="arial", shape="rect"
                    )

                    if name in highlight:
                        color = "#7dc242"
                    elif name in unbound:
                        color = "red"
                    elif name in self._parameters:
                        color = "lightblue"
                    else:
                        color = "lightgrey"

                    if color:
                        node_styles.update(fillcolor=color)

                    node_styles.update(extra_node_styles.get(v, {}))

                    if flatten:
                        label = name.replace("_", "\n")
                    else:
                        label = k.replace("_", "\n")

                    if name in filter:
                        g.node(name, label=label, **node_styles)
                else:
                    g.subgraph(create_subgraph(v, k))
            return g

        g = create_subgraph(tree)
        g.attr("graph", rankdir="BT")
        for node in self.dag().nodes():
            for _, pred in self._resolve_predecessors(node):
                if (not hide_parameters or pred not in self._parameters) and (
                    pred in filter and node in filter
                ):
                    g.edge(pred, node)

        return g

    def _build_name_tree(self):
        # Build up a tree of the subgraphs
        def recursive_tree():
            return defaultdict(recursive_tree)

        tree = recursive_tree()
        unbound = set(self._unbound().keys())

        for node in [*self.dag().nodes(), *unbound]:
            root = tree
            parts = node.split("__")
            for part in parts[:-1]:
                root = tree[part]
            root[parts[-1]] = node
        return tree

    def _unbound(self):

        return {
            k: [t for _, t in v]
            for k, v in groupby(
                sorted(
                    [
                        (arg, key)
                        for key in self._functions
                        for _, arg in self._resolve_predecessors(key)
                        if arg not in self._functions
                    ]
                ),
                key=lambda t: t[0],
            )
        }

    def _resolve_predecessor(self, fname, pname):
        fparts = fname.split("__")[:]
        possible_preds = [
            "__".join(fparts[:i] + [pname]) for i in range(0, len(fparts))
        ]
        possible_preds.reverse()
        for possible in possible_preds:
            if possible in self._links:
                return self._links[possible]

            if possible in self._functions:
                return possible
        else:
            return possible_preds[0]

    def _resolve_predecessors(self, fname):

        if fname not in self._functions:
            return []

        fn = self._functions[fname]
        for pname, parameter in signature(fn).parameters.items():
            resolved_name = self._resolve_predecessor(fname, pname)
            if parameter.default is not Parameter.empty:
                if resolved_name in self._functions:
                    yield pname, resolved_name
            else:
                yield pname, resolved_name

