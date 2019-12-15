#%%
from fn_graph import Composer
from textwrap import dedent
from random import randint, choice
from functools import reduce
from .utils import compare_composer_results

#%%
def random_graph(graph_size=50):

    functions = []

    def function_0():
        return 42

    functions.append(function_0)

    for i in range(1, graph_size):
        fn_name = f"function_{i}"
        num_args = randint(0, min(5, i - 1))
        arg_names = set()

        while len(arg_names) < num_args:
            arg_name = f"function_{randint(0, i-1)}"
            if arg_name not in arg_names:
                arg_names.add(arg_name)

        if arg_names:
            body = " + ".join(arg_names)
        else:
            body = str(randint(0, 100))

        exec(
            dedent(
                f"""
            def {fn_name}({', '.join(sorted(arg_names))}):
                return {body}
            """
            )
        )
        functions.append(locals()[fn_name])

    return Composer().update(*functions)


def test_same_graphs():
    for i in range(10):
        root = random_graph()
        composers = [root, root, root]
        compare_composer_results(root, composers)


def test_random_graphs():
    for i in range(10):
        root = random_graph()
        composers = [root, root.cache()]
        compare_composer_results(root, composers)


# %%
