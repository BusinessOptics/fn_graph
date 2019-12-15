from random import randint, choice


def check_all_equal(values):
    return any([a == b for a, b in list(zip(values[:-1], values[1:]))])


def check_results_equal(results):
    if not check_all_equal([set(result.keys()) for result in results]):
        raise Exception(f"Keys differ")

    for key in results[0]:
        result_values = [result[key] for result in results]
        if not check_all_equal(result_values):
            raise Exception(f"Difference found in {key}, results: {results_values}")


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
            composer.cache_invalidate_from(choice(list(nodes)))
