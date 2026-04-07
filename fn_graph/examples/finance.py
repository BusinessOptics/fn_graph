"""
In finance, the Sharpe ratio measures the performance of an investment compared
to a risk-free asset, after adjusting for its risk.

This shows how to calculate the Sharpe ratio for a small portfolio of shares.
"""

from datetime import date
from math import sqrt

import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf
from pandas.plotting import register_matplotlib_converters

from fn_graph.examples.solution.composer import PipelineComposer as Composer

register_matplotlib_converters()
plt.style.use("fivethirtyeight")


def closing_prices(share_allocations, start_date, end_date):
    data = yf.download(
        " ".join(share_allocations.keys()), start=start_date, end=end_date
    )
    return data["Close"]


def normalised_returns(closing_prices):
    return closing_prices / closing_prices.iloc[0, :]


def positions(normalised_returns, share_allocations, initial_total_position):
    allocations = pd.DataFrame(
        {
            symbol: normalised_returns[symbol] * allocation
            for symbol, allocation in share_allocations.items()
        }
    )
    return allocations * initial_total_position


def total_position(positions):
    return positions.sum(axis=1)


def positions_plot(positions):
    return positions.plot.line(figsize=(10, 8))


def cumulative_return(total_position):
    return 100 * (total_position.iloc[-1] / total_position.iloc[0] - 1)


def daily_return(total_position):
    return total_position.pct_change(1)


def sharpe_ratio(daily_return):
    return daily_return.mean() / daily_return.std()


def annual_sharpe_ratio(sharpe_ratio, trading_days_in_a_year=252):
    return sharpe_ratio * sqrt(trading_days_in_a_year)


composer = (
    Composer()
    .update(
        closing_prices,
        normalised_returns,
        positions,
        total_position,
        positions_plot,
        cumulative_return,
        daily_return,
        sharpe_ratio,
        annual_sharpe_ratio,
    )
    .update_parameters(
        share_allocations={"AAPL": 0.25, "MSFT": 0.25, "ORCL": 0.30, "IBM": 0.2},
        initial_total_position=100_000_000,
        start_date="2017-01-01",
        end_date=date.today(),
    )
    .cache()
)

f = composer
