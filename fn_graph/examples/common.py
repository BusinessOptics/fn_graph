import sys
from pathlib import Path
import random
from random import choice, random
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent.resolve()))

from fn_graph import Composer

# Introductory example


def a():
    return 5


def b(a):
    return a * 5


def c(a, b):
    return a * b


composer = Composer().update(a, b, c)

# Call any result
composer.c()  # 125
composer.a()  # 5

composer.graphviz().render("intro.gv", format="png")
# Some pure functions


def get_car_prices():
    df = pd.DataFrame(
        dict(
            model=[choice(["corolla", "beetle", "ferrari"]) for _ in range(10)],
            price=[random() * 100_000 + 50000 for _ in range(10)],
        )
    )
    return df


def get_mean_car_price(car_prices, season="summer"):
    if season != "summer":
        return car_prices.price.mean() / 2
    else:
        return car_prices.price.mean()


def get_cheaper_cars(car_prices, your_car_price):
    df = car_prices
    return df[df.price < your_car_price]


def get_savings_on_cheaper_cars(cheaper_cars, mean_car_price):
    return cheaper_cars.assign(savings=lambda df: mean_car_price - df.price)


# Compose the functions
g = (
    Composer()
    .update_without_prefix(
        "get_",
        get_car_prices,
        get_cheaper_cars,
        get_mean_car_price,
        get_savings_on_cheaper_cars,
    )
    .update_parameters(your_car_price=100_000)
)
# Calculate a function
print(g.calculate(["savings_on_cheaper_cars"]))

# Or many functions
print(g.calculate(["savings_on_cheaper_cars", "mean_car_price"]))

# Or use the shorthand

print(g.savings_on_cheaper_cars())

# Override a default argument
g.update(season=lambda: "spring").cheaper_cars()
print(g.savings_on_cheaper_cars())

# Lets create some more functions and create a partial composition


def burger_savings(savings_df, price_of_a_burger):
    return savings_df.assign(burgers_saved=lambda df: df.savings / price_of_a_burger)


f = Composer().update(price_of_a_burger=lambda: 100).update(burger_savings)

# We can see we have some errors
print(list(f.check()))


# Lets compose g and f together
# We also add another function to bridge the name

h = g.update_from(f).update(
    savings_df=lambda savings_on_cheaper_cars: savings_on_cheaper_cars
)

# see no errors
print(list(h.check()))


# Lets calculate the result while logging the progress
def log_progress(event, details):
    time = f'{details.get("time"): .5f}' if "time" in details else ""
    print(f"{event:20s} {details.get('fname','')[:50]:50} {time}")


print(
    h.update(season=lambda: "winter").calculate(
        ["burger_savings"], progress_callback=log_progress
    )["burger_savings"]
)


# You can't have cycles
cycles = Composer().update(a=lambda c: c, b=lambda a: a, c=lambda b: b)
print(list(cycles.check()))


# Lets create a new composer using namespaces
# We also link the ideas that don't have matching names
# We use "__" to separate namespace components
h = (
    Composer()
    .update_namespaces(cars=g, burgers=f)
    .link(burgers__savings_df="cars__savings_on_cheaper_cars")
)


# There is a lot you can do with name spaces.
h.calculate(["burgers__burger_savings"])

# If you draw the graph you can see the namespaces
h.graphviz()

g.calculate(["savings_on_cheaper_cars"], intermediates=False)

# %%
