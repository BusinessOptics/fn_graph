"""
A credit model that uses a machine learning model to estimate the expected 
value of the the remaining loans in a loan book.

The example shows how to integrate a machine learning model with some simple 
domain information to get contextualized result. It also show cases how statistical
libraries like Seaborn can be used to investigate data before hand. 
"""

from pathlib import Path

import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier

from fn_graph import Composer

data_path = Path(__file__).parent


def loan_data():
    """
    Load the loan data
    """
    return pd.read_csv(data_path / "credit_data.csv")


def training_data(loan_data):
    """
    Ignore currently live loans
    """
    return loan_data[loan_data.status != "LIVE"]


def investigate_data(training_data):
    """
    Use a seaborn pairplot to get a feel for the data
    """
    return sns.pairplot(training_data.sample(100), hue="status")


def training_features(training_data: pd.DataFrame):
    """
    One hot encode gender and dro =p columns not used in training
    """
    return pd.get_dummies(
        training_data.drop(columns=["outstanding_balance", "status", "account_no"])
    )


def training_target(training_data):
    """
    Convert the target variable to a boolean
    """
    return training_data.status == "DEFAULT"


def model(training_features, training_target):
    """
    Fit a model
    """
    model = RandomForestClassifier()
    model.fit(training_features, training_target)
    return model


def prediction_data(loan_data):
    """
    Only consider the currently live loans
    """
    return loan_data[loan_data.status == "LIVE"]


def prediction_features(prediction_data: pd.DataFrame):
    """
    Prepare the prediction features
    """
    return pd.get_dummies(
        prediction_data.drop(columns=["outstanding_balance", "status", "account_no"])
    )


def probability_of_default(model, prediction_features):
    """
    Predict the probability of default using the trained model
    """
    return model.predict_proba(prediction_features)[:, 1]


def expected_outstanding_repayment(prediction_data, probability_of_default):
    """
    Calculate the expected repayment for each loan
    """
    return prediction_data.assign(probability_of_default=probability_of_default).assign(
        expected_repayment=lambda df: df.outstanding_balance
        * (1 - df.probability_of_default)
    )


def value_of_live_book(expected_outstanding_repayment):
    """
    The total remianing value of the loan book
    """
    return expected_outstanding_repayment.expected_repayment.sum()


composer = Composer().update(
    loan_data,
    training_data,
    training_features,
    training_target,
    investigate_data,
    model,
    prediction_data,
    prediction_features,
    probability_of_default,
    expected_outstanding_repayment,
    value_of_live_book,
)

# Just for uniformity with the rest of the examples
f = composer
