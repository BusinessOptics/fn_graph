"""
A simple machine learning example that builds a classifier for the standard iris dataset.
"""

import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model, sklearn.metrics
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pylab as plt

from fn_graph.examples.solution.composer import PipelineComposer as Composer


def iris():
    return sklearn.datasets.load_iris()


def data(iris):
    df_train = pd.DataFrame(
        iris.data, columns=["feature{}".format(i) for i in range(4)]
    )
    return df_train.assign(y=iris.target)


def investigate_data(data):
    return sns.pairplot(data, hue="y")


def preprocess_data(data, do_preprocess):
    processed = data.copy()
    if do_preprocess:
        processed.iloc[:, :-1] = sklearn.preprocessing.scale(processed.iloc[:, :-1])
    return processed


def split_data(preprocess_data):
    return dict(
        zip(
            ("training_features", "test_features", "training_target", "test_target"),
            train_test_split(preprocess_data.iloc[:, :-1], preprocess_data["y"]),
        )
    )


def training_features(split_data):
    return split_data["training_features"]


def training_target(split_data):
    return split_data["training_target"]


def test_features(split_data):
    return split_data["test_features"]


def test_target(split_data):
    return split_data["test_target"]


def model(training_features, training_target, model_type):
    if model_type == "ols":
        m = sklearn.linear_model.LogisticRegression()
    elif model_type == "svm":
        m = sklearn.svm.SVC()
    else:
        raise ValueError("invalid model selection, choose either 'ols' or 'svm'")
    m.fit(training_features, training_target)
    return m


def predictions(model, test_features):
    return model.predict(test_features)


def classification_metrics(predictions, test_target):
    return sklearn.metrics.classification_report(test_target, predictions)


def plot_confusion_matrix(cm, target_names, title="Confusion matrix", cmap=plt.cm.Blues):
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
    cm = sklearn.metrics.confusion_matrix(test_target, predictions)
    return plot_confusion_matrix(cm, ["setosa", "versicolor", "virginica"])


f = (
    Composer()
    .update_parameters(
        model_type="ols",
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
