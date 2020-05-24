"""
In finance, the Sharpe ratio (also known as the Sharpe index, the Sharpe measure, 
and the reward-to-variability ratio) measures the performance of an investment 
(e.g., a security or portfolio) compared to a risk-free asset, after adjusting 
for its risk. It is defined as the difference between the returns of the 
investment and the risk-free return, divided by the standard deviation of the 
investment (i.e., its volatility). It represents the additional amount of return 
that an investor receives per unit of increase in risk.

This shows how to calculate a the Sharoe ratio for a small portfolio of shares. The
share data us pulled from yahoo finance and the analysis is done in pandas. We assume 
a risk free rate of zero.
"""

from datetime import date
from math import sqrt
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf
from fn_graph import Composer
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()
plt.style.use("fivethirtyeight")


def closing_prices(share_allocations, start_date, end_date):
    """
    The closing prices of our portfolio pulled with yfinance.
    """
    data = yf.download(
        " ".join(share_allocations.keys()), start=start_date, end=end_date
    )
    return data["Close"]


def normalised_returns(closing_prices):
    """
    Normalise the returns as a ratio of the initial price.
    """
    return closing_prices / closing_prices.iloc[0, :]


def positions(normalised_returns, share_allocations, initial_total_position):
    """
    Our total positions ovr time given an initial allocation.
    """

    allocations = pd.DataFrame(
        {
            symbol: normalised_returns[symbol] * allocation
            for symbol, allocation in share_allocations.items()
        }
    )

    return allocations * initial_total_position


def total_position(positions):
    """
    The total value of out portfolio
    """
    return positions.sum(axis=1)


def positions_plot(positions):
    return positions.plot.line(figsize=(10, 8))


def cumulative_return(total_position):
    """
    The cumulative return of our portfolio
    """
    return 100 * (total_position[-1] / total_position[0] - 1)


def daily_return(total_position):
    """
    The daily return of our portfolio
    """
    return total_position.pct_change(1)


def sharpe_ratio(daily_return):
    """
    The sharpe ratio of the portfolio assuming a zero risk free rate.
    """
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

# Just for uniformity with the rest of the examples
f = composer
