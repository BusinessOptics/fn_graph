"Examples of various different supported plotting libraries."
#%%
import math

import matplotlib.pylab as plt
import plotly.express as px
import seaborn as sns
from fn_graph import Composer


def matplotlib_plot(data):
    fig2, ax = plt.subplots()
    ax.plot(data.total_bill, data.tip, "o")
    return fig2


def seaborn_plot(data):

    return sns.relplot(
        x="total_bill",
        y="tip",
        col="time",
        hue="smoker",
        style="smoker",
        size="size",
        data=data,
    )


def pandas_plot(data):
    return data.total_bill.plot.hist()


def plotly_plot(data):
    return px.scatter(
        data,
        x="total_bill",
        y="tip",
        color="sex",
        facet_row="time",
        facet_col="day",
        trendline="ols",
        size="size",
        symbol="smoker",
    )


f = (
    Composer()
    .update_parameters(data=sns.load_dataset("tips"))
    .update(matplotlib_plot, seaborn_plot, pandas_plot, plotly_plot)
)
