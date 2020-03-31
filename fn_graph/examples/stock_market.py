"""
A simple example that works out the market capitalization fo a couple of stocks.
"""
# %%
from fn_graph import Composer
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.express as px

data_path = Path(__file__).parent


def share_prices():
    """
    Load the share price data
    """
    return pd.read_csv(data_path / "share_prices.csv", parse_dates=["datetime"])


def shares_in_issue():
    """
    Load the shares issued data
    """
    return pd.read_csv(data_path / "shares_in_issue.csv", parse_dates=["datetime"])


def daily_share_prices(share_prices):
    """
    Ensure that every day has the full set of share prices
    """
    return (
        share_prices.groupby("share_code")
        .apply(lambda df: df.set_index("datetime").resample("1D").ffill().reset_index())
        .reset_index(drop=True)
        .sort_values(by=["datetime", "share_code"])
    )


def market_cap(daily_share_prices, shares_in_issue):
    """
    Merge the datasets intelligently over time and calculate market cap
    """
    return pd.merge_asof(
        daily_share_prices, shares_in_issue, on="datetime", by="share_code"
    ).assign(market_cap=lambda df: df.share_price * df.shares_in_issue)


def total_market_cap(market_cap):
    """
    Workout the total market cap
    """
    return market_cap.groupby("datetime", as_index=False).market_cap.sum()


def total_market_cap_change(total_market_cap, swing_threshold):
    """
    Calculate the changes in market cap
    """
    return total_market_cap.assign(
        market_cap_change=lambda df: df.market_cap.diff()
    ).assign(
        change_classification=lambda df: np.where(
            np.abs(df.market_cap_change) > swing_threshold, "large", "small"
        )
    )


def plot_market_caps(market_cap):
    """
    Plot the individual market caps
    """
    return px.area(
        market_cap,
        x="datetime",
        y="market_cap",
        facet_row="share_code",
        color="share_code",
    )


def plot_total_market_cap(total_market_cap):
    """
    Plot the total market cap
    """
    return px.line(total_market_cap, x="datetime", y="market_cap")


def plot_market_cap_changes(total_market_cap_change):
    """
    Plot the market cap changes
    """
    return px.bar(
        total_market_cap_change,
        x="datetime",
        y="market_cap_change",
        color="change_classification",
    )


f = (
    Composer()
    .update_parameters(swing_threshold=0.1 * 10 ** 12)
    .update(
        share_prices,
        daily_share_prices,
        shares_in_issue,
        market_cap,
        total_market_cap,
        total_market_cap_change,
        plot_market_caps,
        plot_total_market_cap,
        plot_market_cap_changes,
    )
)
