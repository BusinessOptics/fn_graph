from .utils import compare_composer_results, generate_random_graph
from random import choice
from fn_graph.tests.large_graph import large_graph
from fn_graph import Composer
from ..calculation import get_execution_instructions

root = large_graph
composers = [root, root.cache(), root.development_cache(__name__)]


def test_static_cache_equality():
    root = large_graph
    composers = [root, root.cache(), root.development_cache(__name__)]
    compare_composer_results(root, composers)


def test_cache_clear():
    for i in range(5):
        for composer in composers:
            composer.call(choice(list(composer.dag().nodes())))
            composer.cache_clear()


def test_cache_graphviz():
    for composer in composers:
        composer.call(choice(list(composer.dag().nodes())))
        composer.cache_invalidate(choice(list(composer.dag().nodes())))
        composer.cache_graphviz()


def test_same_graphs():
    for i in range(10):
        root = generate_random_graph()
        composers = [root, root, root]
        compare_composer_results(root, composers)


def test_random_graphs():
    for i in range(10):
        root = generate_random_graph()
        composers = [root, root.cache()]
        compare_composer_results(root, composers)


def test_cache_invalidation():
    composer = Composer().update_parameters(a=1).cache()
    assert composer.a() == 1
    composer = composer.update_parameters(a=2)
    assert composer.a() == 2
