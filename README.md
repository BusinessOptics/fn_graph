# Fn Graph

Light weight function pipelines for python.

## Overview

`fn_graph` is trying to solve a number of problems in the python data-science/modelling domain, as well as making it easier to put such models into production.

It aims to:

1. Make moving between the analyst space to production. amd back, simpler and less error prone.
2. Make it easy to view the intermediate results of computations to easily diagnose errors.
3. Solve common analyst issues like creating reusable, composable pipelines and caching results.
4. Visualizing models in an intuitive way.

There is an associated visual studio you should check out at https://github.com/BusinessOptics/fn_graph_studio/.

## Documentation

Please find detailed documentation at https://fn-graph.readthedocs.io/

## Installation

```
pip install fn_graph
```

You will need to have graphviz and the development packages installed. On ubuntu you can install these with:

```
sudo apt-get install graphviz graphviz-dev
```

Otherwise see the [pygraphviz documentation](http://pygraphviz.github.io/documentation/pygraphviz-1.5/install.html).

## Outline of the problem

In complex domains, like financial modelling or data-science, modellers and programmers often have complex logic that needs to be re-used in multiple environments. For example a financial analyst may write a model in a notebook, which then must be placed in a production environment (normally through rewriting the notebook which was not written in a modular or easy to use manner), after which changes need to made, which must be done in the notebook environment, which then need to be moved back into production. This is the stuff of [nightmare fuel](https://www.urbandictionary.com/define.php?term=nightmare%20fuel) for everyone involved.

To make this process easier the best option is to break functionality into small reusable functions, that can be referenced in both the notebook environment and the production environment. Now you unfortunately you have this bag of small functions whose relationships you have to manage. There are few ways to do this. Assume we have the functions below:

```python
def get_a():
    return 5

def get_b(a):
    return a * 5

def get_c(a, b):
    return a * b
```

**Option 1 - Directly compose at call site:**

```python
a = get_a()
b = get_b(a)
c = get_c(a, b)
```

_Pros:_ The modeller can easily see intermediate results\
_Cons:_ Now this potentially complex network of function calls has to be copied and pasted between notebooks and production

**Option 2 - Wrap it up in a function that pass around**

```python
def composed_c():
    a = get_a()
    b = get_b(a)
    return  get_c(a, b)
```

_Pros:_ This is easy to reference from both the notebook and production\
_Cons:_ The modeller cannot see the intermediate results, so it can be difficult to debug (this is a big problem if you are working with big multi-dimensional objects that you may want to visualize, a debugger does not cut it)

**Option 3 - Directly call functions from each other**

```python
def get_a():
    return 5

def get_b(a):
    return get_a() * 5

def get_c():
    return get_a() * get_b()
```

_Pros:_ This is easy to reference from both the notebook and production\
_Cons:_ The modeller cannot see the intermediate results and functions cannot be reused. Functions are called multiple times.

None of these are great. Fn Graph would solve it like this.

```python
from fn_graph import Composer

def a():
    return 5

def b(a):
    return a * 5

def c(a, b):
    return a * b

composer = Composer().update(a, b, c)

# Call any result
composer.c() # 125
composer.a() # 5

composer.graphviz()
```

![Graph of composer](intro.gv.png)

The composer can then be easily passed around in both the production and notebook environment. It can do much more than this.

## Features

- Manage complex function graphs, including using namespaces.
- Update composers to gradually build more and more complex logic.
- Enable incredible function reuse.
- Visualize logic to make knowledge sharing easier.
- Perform graph operations on composers to dynamically rewire your logic.
- Manage calculation life cycle, with hooks, and have access to all intermediary calculations.
- Cache results, either within a single session, or between sessions in development mode. Using the development cache intelligently invalidate the cache when code changes .

## Similar projects

**Dask**

Dask is a light-weight parallel computing library. Importantly it has a Pandas compliant interface. You may want to use Dask inside FnGraph.

**Airflow**

Airflow is a task manager. It is used to run a series of generally large tasks in an order that meets their dependencies, potentially over multiple machines. It has a whole scheduling and management apparatus around it. Fn Graph is not trying to do this. Fn Graph is about making complex logic more manageable, and easier to move between development and production. You may well want to use Fn Graph inside your airflow tasks.

**Luigi**

> Luigi is a Python module that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization etc. It also comes with Hadoop support built in.

Luigi is about big batch jobs, and managing the distribution and scheduling of them. In the same way that airflow works ate a higher level to FnGraph, so does luigi.

**d6tflow**

d6tflow is very similar to FnGraph. It is based on Luigi. The primary difference is the way the function graphs are composed. d6tflow graphs can be very difficult to reuse (but do have some greater flexibility). It also allows for parallel execution. FnGraph is trying to make very complex pipelines or very complex models easier to mange, build, and productionise.
