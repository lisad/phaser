# Advanced Usage

More stuff about more advanced usage of Phaser

(custom-column-validation)=
## Advanced Column Validation

Builtin columns ([Column](#Column), [IntColumn](#IntColumn), [FloatColumn](#FloatColumn),
[DateColumn](#DateColumn), etc.) can all be instantiated with parameters to do
the most common validations, including whether the column must be in the data, whether values can be null or blank,
minimum/maximum values for scalar fields, and allowed values for enumerated fields.

For going beyond this, consider a custom column.  By overriding the _check_value_ method, you can do validation that
depends only on the column itself - thus, note that any validation that requires looking at TWO columns (e.g. ensuring 
end_date is after start_date) should be done in a step that deals with a whole row.

```python
from phaser import Column, ON_ERROR_DROP_ROW, Phase, Pipeline
from core.models import Account

class ValidUserAccountIdColumn(Column):
    def check_value(self, value):
        super().check_value(value)
        if not Account.objects.filter(account_id=value).exists():
            raise Exception(f"User account {value} not found in account database")
        if not Account.objects.get(account_id=value).status == "Active":
            raise Exception(f"User account {value} is not Active")

class SupportMessageRelay(Pipeline):
    phases = [
        Phase(
            columns = [
                ValidUserAccountIdColumn(name='id', required=True, on_error=ON_ERROR_DROP_ROW),
                Column(name='message', required=True)
            ],
            steps = [ """ Steps to clean data etc """]
        )
    ]

```

The above example for use in a django context (the import of an 'Account' model would allow the accounts table to
be queried, assuming the server has its db connection) creates a reusable ValidUserAccountIdColumn, that can be used
for a number of cases of checking pipeline data for a valid account ID.  It can be used when relaying support messages,
and support messages without valid account IDs would be dropped according to the 'on_error' policy defined for that
phase.

## Reshaping

## Error policies

## Changing the output of logging

## Context steps

## Accessing additional sources -- side data

## Exporting additional outputs

## Piping data between phases

When data is mostly just in one table, there's no need to do anything special to pipe data between phases. When
the EmployeePipeline in the readme is run, the source data for the run is loaded into the pipeline and passed to
the first phase, __Validator__.  The output of __Validator__'s run method is saved and also passed as input to the
second phase, __Transformer__.

If additional source data is needed in one or more phases, it can be named as an extra source (see above) and when
additional outputs are created, they can be named as extra outputs.  However, a special case arises when a phase
has more than one output or when output needs to be used not by the next phase but by a later phase.  In the
example below, the CleanEmployeeData phase not only outputs an employee table, which is by default passed to the
CalculateBonuses phase, it also outputs a manager table.  Finally, the SumBonuses phase needs the manager table.

```python
from collections import defaultdict
from phaser import Phase, Pipeline, row_step, dataframe_step, ExtraMapping

@row_step(extra_outputs=[ 'manager_list' ])
def collect_employees_by_manager(row, manager_list):
    manager_list[row['manager']].append(row['employee_id'])
    return row

class CleanEmployeeData(Phase):
    steps = [ collect_employees_by_manager]   # Plus other steps as needed ...
    extra_outputs = [ ExtraMapping('manager_list', defaultdict(list)) ]

class CalculateBonuses(Phase):
    steps = [ ]    # fun complicated steps not shown

@dataframe_step(extra_sources=['manager_list'])
def sum_bonuses(dataframe, manager_list):
    dataframe.bonuses  # ... some clever logic to sum bonuses by employees in the manager list
    return dataframe

class SumBonuses(Phase):
    steps = [ sum_bonuses ]    # plus other steps not shown ... 
    extra_sources = ['manager_list']

class EmployeeDataPipeline(Pipeline):
    phases = [
        CleanEmployeeData,
        CalculateBonuses,
        SumBonuses
    ]

```

Note above that the step _collect_employees_by_manager_ declares that it produces extra output data, as well as the
phase it is in; similarly the step _sum_bonuses_ declares that it uses an extra source table, and so does the phase it
is in.

## Testing the Outgoing Data Contract

Defining the entire set of columns for a table of data can be a great way to test incoming data to see if it meets a
data contract.  But how can we test outgoing data to see if it meets the data contract?  This kind of defensive
programming can save time debugging where a problem occurred by reporting it earlier, where it happened.

In phaser, column validation only happens at the beginning of a Phase, however, because many Phase operations will
add fields, drop fields, or even dramatically reshape data, and the outgoing columns are often not quite the same
as the incoming columns.  In fact the data should now meet a stricter contract if it has been cleaned up nicely.

To do this for a whole pipeline, we recommend adding a Phase at the end that only defines the contract for the output
of the pipeline, and it will check before completing and report all errors assuming the pipeline got that far.

```python
from phaser import Pipeline, Phase, Column, FloatColumn
from tests.pipelines.employees import Validator, Transformer

class ContractChecker(Phase):
    columns = [
        Column('Employee ID', required=True, blank=False),
        FloatColumn('Pay rate', required=True, blank=False, min_value=0.01),
        FloatColumn('Salary', required=True, blank=False, min_value=0.01),
        FloatColumn('Bonus percent', required=True, blank=False, min_value=0)
    ]

class StrictEmployeePipeline(Pipeline):
    phases = [Validator,
              Transformer,
              ContractChecker]

```

If you desire contract checking after every phase, any number of phases that check the data contract at that point
can be added in.

See also [custom validation for columns](#custom-column-validation).


## Debugging
