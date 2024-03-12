"""

This file will serve both as an end-to-end test of phaser, as well as an example with annotated documentation of
how to use phaser.

A common integration story in industry is a process, analysis or app that needs to gather data about employees
from multiple sources.  HR systems often have exports, but don't always have very usable APIs.  Ideally this
example will show a convergence pipeline - data from multiple sources is handled in their own phases, then
combined into a single dataset.  We haven't yet built convergence into the Pipeline object so the first
version of this will have to be linear.
* In the next iteration it would be really nice to showcase parsing fields like "$182,000.49" and how to get
exact decimal versions

"""
from collections import defaultdict
from phaser import (
    Phase,
    Pipeline,
    Column,
    FloatColumn,
    IntColumn,
    row_step,
    context_step,
    check_unique,
    DataErrorException,
    DropRowException
)


"""
import employee data fixture  (change the other tests to use a 'crew' fixture)

1 phase to Validate data to start with - so one data file must have errors and fail the validate step
1 phase to transform employee data to a common format
1 phase to import performance review data
1 phase to check against known departments and make a list of new departments?

"""


@row_step
def drop_rows_with_no_id_and_not_employed(row, **kwargs):
    if not row["Employee ID"]:
        if row['Status'] == "Active":
            raise DataErrorException("Missing employee ID for active employee, need to followup")
        elif row['Status'] == "Inactive":
            raise DropRowException(f"Employee {row['Last name']} has no ID and inactive, dropping row")
        else:
            raise DataErrorException(f"Unknown employee status {row['Status']}")
    return row


@row_step
def combine_full_name(row, **kwargs):
    row["Full name"] = f"{row['First name']} {row['Last name']}"
    return row


@row_step
def calculate_annual_salary(row, **kwargs):
    rate = row['Pay rate']
    match row['Pay period']:
        case "Hour": row['salary'] = rate * 40*52
        case "Day": row['salary'] = rate * 5 * 52
        case "Week": row['salary'] = rate * 52
        case "Month": row['salary'] = rate * 12
        case "Year": row['salary'] = rate
        case _: row['salary'] = 0
    return row


@row_step
def calculate_bonus_percent(row, **kwargs):
    if row.get('bonusAmount') and row['salary'] > 0:
        row["Bonus percent"] = row['bonusAmount'] / row['salary']
    return row

@context_step
def index_departments(context):
    """ Transforms the 'departments' source from a list of records as it came
    from the loaded file into a dictionary of name to record."""
    # TODO: Make this functionality be a built-in feature of Phaser
    lookup_departments = context.get_source('departments')
    departments = {
        r['name']: r for r in lookup_departments
    }
    # Overwrite the existing departments source. Is this ok?
    context.set_source('departments', departments)

@row_step
def add_department_id(row, context):
    lookup_departments = context.get_source('departments')
    department_names = lookup_departments.keys()
    if row['department']:
        if row['department'] in department_names:
            row['department_id'] = lookup_departments[row['department']]['id']
        else:
            context.add_warning(add_department_id, row,
                f"Department name {row['department']} invalid for employee ID {row['Employee ID']}")
    else:
        context.add_warning(add_department_id, row,
            f"Department name missing for employee ID {row['Employee ID']}")

    return row

@row_step
def identify_managers(row, context):
    managers = context.get('managers')
    manager_id = row['manager_id']
    if manager_id:
        managers[manager_id] += 1
    return row

@context_step
def reformat_managers(context):
    managers = context.get('managers')
    rows = [
        { 'manager_id': key, 'num_employees': value }
        for key, value in managers.items()
    ]
    context.add_output('managers', rows)

class Validator(Phase):
    columns = [
        Column(name="Employee ID", rename="employeeNumber"),
        Column(name="First name", rename="firstName"),
        Column(name="Last name", rename="lastName", blank=False),
        FloatColumn(name="Pay rate", min_value=0.01, rename="payRate", required=True),
        Column(name="Pay type",
               rename="payType",
               allowed_values=["hourly", "salary", "exception hourly", "monthly", "weekly", "daily"],
               on_error=Pipeline.ON_ERROR_DROP_ROW,
               save=False),
        Column(name="Pay period", rename="paidPer")
    ]
    steps = [
        drop_rows_with_no_id_and_not_employed,
        check_unique("Employee ID")
    ]


class Transformer(Phase):
    columns = [
        FloatColumn(name='Pay rate'),
        FloatColumn(name="bonusAmount")
    ]
    steps = [
        combine_full_name,
        calculate_annual_salary,
        calculate_bonus_percent,
        index_departments,
        add_department_id
    ]
    extra_sources = [
        'departments'
    ]

class ManagerPhase(Phase):
    columns = [
        IntColumn(name='manager_id')
    ]
    steps = [
        identify_managers,
        reformat_managers
    ]
    extra_outputs = [
        'managers'
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize a dict to hold the manager counts
        self.context.add_variable('managers', defaultdict(int))

class EmployeeReviewPipeline(Pipeline):
    phases = [
        Validator,
        Transformer,
        ManagerPhase
    ]
