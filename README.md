# Fn Graph

Lightweight function pipelines for python

For more information and live examples look at [fn-graph.businessoptics.biz](https://fn-graph.businessoptics.biz/)

## Overview

`fn_graph` is trying to solve a number of problems in the python data-science/modelling domain, as well as making it easier to put such models into production.

It aims to:

1. Make moving between the analyst space to production, and back, simpler and less error prone.
2. Make it easy to view the intermediate results of computations to easily diagnose errors.
3. Solve common analyst issues like creating reusable, composable pipelines and caching results.
4. Visualizing models in an intuitive way.

There is an associated visual studio you should check out at https://github.com/BusinessOptics/fn_graph_studio/.

## Documentation

Please find detailed documentation at https://fn-graph.readthedocs.io/

## Installation

```sh
pip install fn_graph
```

You will need to have graphviz and the development packages installed. On ubuntu you can install these with:

```sh
sudo apt-get install graphviz graphviz-dev
```

Otherwise see the [pygraphviz documentation](http://pygraphviz.github.io/documentation/pygraphviz-1.5/install.html).

To run all the examples install

```sh
pip install fn_graph[examples]
```

## Features

* **Manage complex logic**\
Manage your data processing, machine learning, domain or financial logic all in one simple unified framework. Make models that are easy to understand at a meaningful level of abstraction.
* **Hassle free moves to production**\
Take the models your data-scientist and analysts build and move them into your production environment, whether thats a task runner, web-application, or an API. No recoding, no wrapping notebook code in massive and opaque functions. When analysts need to make changes they can easily investigate all the models steps.
* **Lightweight**\
Fn Graph is extremely minimal. Develop your model as plain python functions and it will connect everything together. There is no complex object model to learn or heavy weight framework code to manage.
* **Visual model explorer**\
Easily navigate and investigate your models with the visual fn_graph_studio. Share knowledge amongst your team and with all stakeholders. Quickly isolate interesting results or problematic errors. Visually display your results with any popular plotting libraries.
* **Work with or without notebooks**\
Use fn_graph as a complement to your notebooks, or use it with your standard development tools, or both.

* **Works with whatever libraries you use**\
fn_graph makes no assumptions about what libraries you use. Use your favorite machine learning libraries like, scikit-learn, PyTorch. Prepare your data with data with Pandas or Numpy. Crunch big data with PySpark or Beam. Plot results with matplotlib, seaborn or Plotly. Use statistical routines from Scipy or your favourite financial libraries. Or just use plain old Python, it's up to you.
* **Useful modelling support tools**\
Integrated and intelligent caching improves modelling development iteration time, a simple profiler works at a level that's meaningful to your model.
** *Easily compose and reuse models**\
The composable pipelines allow for easy model reuse, as well as building up models from simpler submodels. Easily collaborate in teams to build models to any level of complexity, while keeping the individual components easy to understand and well encapsulated.
* **It's just Python functions**\
It's just plain Python! Use all your existing knowledge, everything will work as expected. Integrate with any existing python codebases. Use it with any other framework, there are no restrictions.

## Similar projects

An incomplete comparison to some other libraries, highlighting the differences:

**Dask**

Dask is a light-weight parallel computing library. Importantly it has a Pandas compliant interface. You may want to use Dask inside FnGraph.

**Airflow**

Airflow is a task manager. It is used to run a series of generally large tasks in an order that meets their dependencies, potentially over multiple machines. It has a whole scheduling and management apparatus around it. Fn Graph is not trying to do this. Fn Graph is about making complex logic more manageable, and easier to move between development and production. You may well want to use Fn Graph inside your airflow tasks.

**Luigi**

> Luigi is a Python module that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization etc. It also comes with Hadoop support built in.

Luigi is about big batch jobs, and managing the distribution and scheduling of them. In the same way that airflow works ate a higher level to FnGraph, so does luigi.

**d6tflow**

d6tflow is similar to FnGraph. It is based on Luigi. The primary difference is the way the function graphs are composed. d6tflow graphs can be very difficult to reuse (but do have some greater flexibility). It also allows for parallel execution. FnGraph is trying to make very complex pipelines or very complex models easier to mange, build, and productionise.
