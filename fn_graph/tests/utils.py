from random import randint, choice
from fn_graph import Composer
from textwrap import dedent


def check_all_equal(values):
    return any([a == b for a, b in list(zip(values[:-1], values[1:]))])


def check_results_equal(results):
    if not check_all_equal([set(result.keys()) for result in results]):
        raise Exception(f"Keys differ")

    for key in results[0]:
        result_values = [result[key] for result in results]
        if not check_all_equal(result_values):
            raise Exception(f"Difference found in {key}, results: {result_values}")


def compare_composer_results(root, composers):
    nodes = root.dag().nodes()
    for j in range(10):
        outputs = set((choice(list(nodes)) for k in range(randint(1, len(nodes)))))
        intermediates = randint(0, 1) == 1
        results = [
            composer.calculate(outputs, intermediates=intermediates)
            for composer in composers
        ]
        check_results_equal(results)

        for composer in composers:
            composer.cache_invalidate(choice(list(nodes)))


def generate_random_graph(graph_size=42):

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
