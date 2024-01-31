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
from phaser import (Phase, Pipeline, Column, FloatColumn, row_step, check_unique,
                    PipelineErrorException, DropRowException)


"""
import employee data fixture  (change the other tests to use a 'crew' fixture)

1 phase to Validate data to start with - so one data file must have errors and fail the validate step
1 phase to transform employee data to a common format
1 phase to import performance review data
1 phase to check against known departments and make a list of new departments?

"""

EID = Column(name="Employee ID")  # Can be blank for now until we check Status field in custom step
FIRSTNAME = Column(name="First name", rename="firstName")
LASTNAME = Column(name="Last name", rename="lastName", blank=False)
PAYTYPE = Column(name="Pay type",
                 allowed_values=["hourly", "salary", "exception hourly", "monthly", "weekly", "daily"],
                 on_error=Pipeline.ON_ERROR_DROP_ROW)
PAIDPER = FloatColumn(name="Paid per", min_value=0.01, rename="paidPer")


@row_step
def drop_rows_with_no_id_and_not_employed(row, **kwargs):
    if not row[EID.name]:
        if row['Status'] == "Active":
            raise PipelineErrorException("Missing employee ID for active employee, need to followup")
        elif row['Status'] == "Inactive":
            raise DropRowException
        else:
            raise PipelineErrorException(f"Unknown employee status {row['Status']}")
    return row


@row_step
def combine_full_name(row, **kwargs):
    row["Full name"] = f"{row[FIRSTNAME.name]} {row[LASTNAME.name]}"
    return row


@row_step
def calculate_annual_salary(row, **kwargs):
    rate = row['payRate']
    match row['paidPer']:
        case "Hour": row['salary'] = rate * 40*52
        case "Day": row['salary'] = rate * 5 * 52
        case "Week": row['salary'] = rate * 52
        case "Month": row['salary'] = rate * 12
        case "Year": row['salary'] = rate
    return row


@row_step
def calculate_bonus_percent(row, **kwargs):
    if row['bonusAmount']:
        row["Bonus percent"] = row['bonusAmount'] / row['salary']
    return row


class Validator(Phase):
    columns = [EID, FIRSTNAME, LASTNAME]
    steps = [
        drop_rows_with_no_id_and_not_employed,
        check_unique(EID)
    ]


class Transformer(Phase):
    columns = []
    steps = [
        combine_full_name,
        calculate_annual_salary,
        calculate_bonus_percent
    ]


class EmployeeReviewPipeline(Pipeline):

    phases = [Validator, Transformer]
