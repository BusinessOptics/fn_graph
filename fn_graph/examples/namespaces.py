"A basic example of namespaces."
#%%
from fn_graph import Composer

# Namespaces example


def data():
    return 5


def b(data, factor):
    return data * factor


def c(b):
    return b


def combined_result(child_one__c, child_two__c):
    pass


child = Composer().update(b, c)
parent = (
    Composer()
    .update_namespaces(child_one=child, child_two=child)
    .update(data, combined_result)
    .update_parameters(child_one__factor=3, child_two__factor=5)
)

# %%
parent.graphviz()

# %%

# Link example


def calculated_factor(data):
    return data / 2


factor_calc = Composer()
factoring = Composer().update(calculated_factor)

linked_parent = (
    Composer()
    .update_namespaces(child_one=child, factoring=factoring)
    .update(data)
    .link(child_one__factor="factoring__calculated_factor")
)

f = linked_parent

# %%
