from fn_graph import Composer


def test_basic_links():
    composer = Composer().update(a=lambda: 5, c=lambda b: b * 2).link(b="a")

    assert composer.c() == 10


def test_updated_from_links():
    composer_a = Composer().update(a=lambda: 5, c=lambda b: b * 2).link(b="a")
    composer_b = Composer().update_from(composer_a).update(d=lambda b: b * 3)
    assert composer_b.d() == 15


def test_updated_from_namespaces():
    composer_child = Composer().update(a=lambda: 5, c=lambda b: b * 2).link(b="a")
    composer = (
        Composer()
        .update_namespaces(x=composer_child, y=composer_child)
        .link(outer_x="x__c", outer_y="y__c")
        .update(final=lambda outer_x, outer_y: outer_x + outer_y)
    )

    assert composer.final() == 20


def test_call_link():
    composer = Composer().update(a=lambda: 5, c=lambda b: b * 2).link(b="a")

    assert composer.b() == 5
