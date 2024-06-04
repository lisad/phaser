# API

## Phaser

```{eval-rst}
.. automodule:: phaser
```

## Classes

```{eval-rst}
.. autoclass:: phaser.Pipeline
  :members:
  :inherited-members:

.. autoclass:: phaser.Phase
  :members:
  :inherited-members:

.. autoclass:: phaser.Context
  :members:
```

## Columns

```{eval-rst}
.. autoclass:: phaser.Column
  :members:
  :inherited-members:

.. autoclass:: phaser.BooleanColumn

.. autoclass:: phaser.IntColumn

.. autoclass:: phaser.FloatColumn

.. autoclass:: phaser.DateColumn

.. autoclass:: phaser.DateTimeColumn
```

## Exceptions

```{eval-rst}
.. autoexception:: phaser.DropRowException

```

## Steps

Steps are defined using decorators.  A step is meant to be a function that
accepts an input and produces an output.

```python
@row_step
def add_an_id(row):
  row['id'] = next_id()
  return row
```

Occasionally extra sources or outputs are needed.  In order to use them, they
must first be defined on the Pipeline as an `ExtraRecords` object or an
`ExtraMapping` object.  The extra sources or outputs must then be declared in
the decorator as in the example below.  In the example, `foo` is an
`ExtraMapping`, which presents to the function as a dictionary, and `bar` is an
`ExtraRecord`, which presents like a list.

```python
@row_step(extra_sources=['foo'], extra_outputs=['bar'])
def do_foo_and_bar(row, foo, bar):
    row['foo'] = foo[row['id']]
    bar.append(row['bar'])
    return row
```

All steps can accept a parameter named `context` which will be the
[Context](#phaser.Context) object for the current run of the pipeline.

```{eval-rst}
.. autodecorator:: phaser.row_step

.. autodecorator:: phaser.batch_step

.. autodecorator:: phaser.context_step

.. autodecorator:: phaser.dataframe_step
```
