import sys
from pathlib import Path
import random
from random import choice, random
import pandas as pd
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent.resolve()))

from fn_graph import Composer

logging.basicConfig(level=logging.DEBUG)
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))


def get_car_prices():
    df = pd.DataFrame(
        dict(
            model=[choice(["corolla", "beetle", "ferrari"]) for _ in range(10)],
            price=[random() * 100_000 + 50000 for _ in range(10)],
        )
    )

    return df


def get_mean_car_price(car_prices, season="summer"):
    if season != "winter":
        return car_prices.price.mean() / 2
    else:
        return car_prices.price.mean()


def get_cheaper_cars(car_prices, your_car_price):
    df = car_prices
    return df[df.price < your_car_price]


def get_savings_on_cheaper_cars(cheaper_cars, mean_car_price):
    return cheaper_cars.assign(savings=lambda df: mean_car_price - df.price)


def get_burger_savings(savings_on_cheaper_cars, price_of_a_burger):
    return savings_on_cheaper_cars.assign(
        burgers_saved=lambda df: df.savings / price_of_a_burger
    )


def get_burger_savings_2(savings_on_cheaper_cars, price_of_a_burger):
    return savings_on_cheaper_cars.assign(
        burgers_saved=lambda df: df.savings / price_of_a_burger + 1
    )


f = (
    Composer()
    .update_without_prefix(
        "get_",
        get_car_prices,
        get_cheaper_cars,
        get_mean_car_price,
        get_savings_on_cheaper_cars,
        get_burger_savings,
    )
    .update_parameters(your_car_price=100_000, price_of_a_burger=10)
)


mem_cache = f.cache()

dev_cache = f.development_cache("burger_savings_composer")

dev_cache.cache_invalidate("car_prices")
dev_cache.cache_graphviz()

# Everything should be calculated
dev_cache.burger_savings()

# Everything should be retrieved from cache
dev_cache.burger_savings()

# Invalidate some things
dev_cache.cache_invalidate("car_prices")

# Some things retrieved from cache
dev_cache.burger_savings()

# Update should trigger a cache invalidation
new_dev_cache = dev_cache.update(burger_savings=get_burger_savings_2)
new_dev_cache.cache_graphviz()
new_dev_cache.burger_savings()
