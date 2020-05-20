"""
A simple machine learning example that builds a classifier for the standard iris dataset.

This example uses scikit-learn to build a a classifier to detect the species of an iris 
flower based on attributes of the flower. Based on the parameters different types of models 
can be trained, and preprocessing can be turned on and off. It also show cases the integration 
of visualisations to measure the accuracy of the model.  
"""

from fn_graph import Composer
import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model, sklearn.metrics
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pylab as plt


def iris():
    """
    Load the classic iris dataset
    """
    return sklearn.datasets.load_iris()


def data(iris):
    """
    Pull out the data as pandas DataFrame
    """
    df_train = pd.DataFrame(
        iris.data, columns=["feature{}".format(i) for i in range(4)]
    )
    return df_train.assign(y=iris.target)


def investigate_data(data):
    """
    Check for any visual correlations using seaborn
    """
    return sns.pairplot(data, hue="y")


def preprocess_data(data, do_preprocess):
    """
    Preprocess the data by scaling depending on the parameter

    We make sure we don't mutate the data because that is better practice.
    """
    processed = data.copy()
    if do_preprocess:
        processed.iloc[:, :-1] = sklearn.preprocessing.scale(processed.iloc[:, :-1])
    return processed


def split_data(preprocess_data):
    """
    Split the data into test and train sets
    """
    return dict(
        zip(
            ("training_features", "test_features", "training_target", "test_target"),
            train_test_split(preprocess_data.iloc[:, :-1], preprocess_data["y"]),
        )
    )


# This is done verbosely purpose, but it could be more concise


def training_features(split_data):
    return split_data["training_features"]


def training_target(split_data):
    return split_data["training_target"]


def test_features(split_data):
    return split_data["test_features"]


def test_target(split_data):
    return split_data["test_target"]


def model(training_features, training_target, model_type):
    """
    Train the model
    """
    if model_type == "ols":
        model = sklearn.linear_model.LogisticRegression()
    elif model_type == "svm":
        model = sklearn.svm.SVC()
    else:
        raise ValueError("invalid model selection, choose either 'ols' or 'svm'")
    model.fit(training_features, training_target)
    return model


def predictions(model, test_features):
    """
    Make some predictions foo the test data
    """
    return model.predict(test_features)


def classification_metrics(predictions, test_target):
    """
    Show some standard classification metrics
    """
    return sklearn.metrics.classification_report(test_target, predictions)


def plot_confusion_matrix(
    cm, target_names, title="Confusion matrix", cmap=plt.cm.Blues
):
    """
    Plots a confusion matrix using matplotlib. 
    
    This is just a regular function that is not in the composer. 
    Shamelessly taken from https://scikit-learn.org/0.15/auto_examples/model_selection/plot_confusion_matrix.html
    """

    plt.imshow(cm, interpolation="nearest", cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(target_names))
    plt.xticks(tick_marks, target_names, rotation=45)
    plt.yticks(tick_marks, target_names)
    plt.tight_layout()
    plt.ylabel("True label")
    plt.xlabel("Predicted label")
    return plt.gcf()


def confusion_matrix(predictions, test_target):
    """
    Show the confusion matrix
    """
    cm = sklearn.metrics.confusion_matrix(test_target, predictions)
    return plot_confusion_matrix(cm, ["setosa", "versicolor", "virginica"])


f = (
    Composer()
    .update_parameters(
        # Parameter controlling the model type (ols, svc)
        model_type="ols",
        # Parameter enabling data preprocessing
        do_preprocess=True,
    )
    .update(
        iris,
        data,
        preprocess_data,
        investigate_data,
        split_data,
        training_features,
        training_target,
        test_features,
        test_target,
        model,
        predictions,
        classification_metrics,
        confusion_matrix,
    )
)
