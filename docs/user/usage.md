# Usage

A whole lot of info on how to use Phaser

## Typed columns

## Row steps

## Batch steps

## DataFrame steps

## Built-in steps

## Running in production

There are two approaches to using Phaser in a production environment --
programmatic invocation or writing scripts that use the command-line interface.

**TODO** Talk about when programmatic invocation makes sense, such as if you are
using a job running or orchestration tool.

Phaser can be launched from any python program by importing your pipeline,
instantiating it with today's data file(s) and working directory, and running
it.  Output will be saved in the working directory along with errors, warnings,
and checkpoints (a copy of the data as it appeared at the end of each phase, so
that changes can be traced back to the phase they occurred in)

As an example, if you have cloned the phaser repository, you can run the
`EmployeeReviewPipeline` in the `tests.pipeline` package.

```python
from tests.pipelines.employees import EmployeeReviewPipeline

pipeline = EmployeeReviewPipeline(
        source='tests/fixture_files/employees.csv',
        working_dir='~/phaser_output'
    )
pipeline.run()
```
