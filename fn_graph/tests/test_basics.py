from fn_graph import Composer


def test_simple_parameters():
    composer = (
        Composer().update(c=lambda a, b: a + b).update_parameters(a=1, b=(int, 2))
    )
    assert composer.c() == 3


def test_default_arguments():
    composer = Composer().update(c=lambda a, b=3: a + b).update_parameters(a=1)
    assert composer.c() == 4
    composer = Composer().update(c=lambda a, b=3: a + b).update_parameters(a=1, b=2)
    assert composer.c() == 3


def test_var_args():
    a = 1
    b = 2
    result = a + sum(a * 2 for i in range(10)) + b + sum(a * 5 for i in range(5))

    def d(a, *d_, b, **c_):
        return a + sum(d_) + b + sum(c_.values())

    composer = (
        Composer()
        .update_parameters(a=1, b=2)
        .update(**{f"c_{i}": lambda a: a * 2 for i in range(10)})
        .update(**{f"d_{i}": lambda a: a * 5 for i in range(5)})
        .update(d)
    )

    assert composer.d() == result


def test_empty_var_args():
    a = 1
    b = 2
    result = a + sum(a * 2 for i in range(10)) + b

    def d(a, *d_, b, **c_):
        return a + sum(d_) + b + sum(c_.values())

    composer = (
        Composer()
        .update_parameters(a=1, b=2)
        .update(**{f"c_{i}": lambda a: a * 2 for i in range(10)})
        .update(d)
    )

    assert composer.d() == result


def test_empty_kwargs():
    a = 1
    b = 2
    result = a + b + sum(a * 5 for i in range(5))

    def d(a, *d_, b, **c_):
        return a + sum(d_) + b + sum(c_.values())

    composer = (
        Composer()
        .update_parameters(a=1, b=2)
        .update(**{f"d_{i}": lambda a: a * 5 for i in range(5)})
        .update(d)
    )

    assert composer.d() == result
