"""
A simple example showing basic functionality.
"""
#%%
from random import choice, random

import pandas as pd
import plotly.express as px
from fn_graph import Composer

prices = [random() * 100_000 + 50000 for _ in range(10)]


def get_car_prices():
    df = pd.DataFrame(
        dict(
            model=[choice(["corolla", "beetle", "ferrari"]) for _ in range(10)],
            price=prices,
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


def get_burger_savings(savings_on_cheaper_cars, price_of_a_burger):
    return savings_on_cheaper_cars.assign(
        burgers_saved=lambda df: df.savings / price_of_a_burger
    )


def get_savings_histogram(burger_savings):
    return px.histogram(burger_savings, x="burgers_saved")


f = (
    Composer()
    .update_without_prefix(
        "get_",
        get_car_prices,
        get_cheaper_cars,
        get_mean_car_price,
        get_savings_on_cheaper_cars,
        get_burger_savings,
        get_savings_histogram,
    )
    .update_parameters(your_car_price=(int, 100_000), price_of_a_burger=(float, 100))
)
