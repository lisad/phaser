from pandas import isna
from .phase import Phase

def is_not_equal(a, b):
    """
    Returns true if a and b are not equal, taking into account pandas.NA and
    numpy.nan, treating NA and nan as equal to themselves and each other
    """
    if a is b:
        return False
    if isna(a) & isna(b):
        return False
    if isna(a) | isna(b):
        return True
    return a != b

def diff_row(a, b, no_key=None):
    # Thank you: https://code.activestate.com/recipes/576644-diff-two-dictionaries/#c9
    both = a.keys() & b.keys()
    diff = {k:(a[k], b[k]) for k in both if is_not_equal(a[k], b[k])}
    diff.update({k:(a[k], no_key) for k in a.keys() - both})
    diff.update({k:(no_key, b[k]) for k in b.keys() - both})
    return diff

# TODO: Actually implement this
def diff_rows(aa, bb):
    return aa + bb

# A Phase that wraps and delegates to another Phase, adding in logic to run
# diffs of the data as it is transformed.
class DiffingPhase(Phase):
    def __init__(self, phase):
        super().__init__(phase.name, phase.steps, phase.columns, phase.context, phase.default_error_policy)

    def execute_row_step(self, step):
        def _step(row, **kwargs):
            # TODO: If the step mutates the row, no diffs will show up. Either
            # send in a deep copy, or capture the mutations through
            # metaprogramming like overwriting `__setitem__`.
            new_row = step(row, **kwargs)
            diff = diff_row(row, new_row)
            if len(diff) > 0:
                print(f"DIFF {step.__name__}: {diff}")
            return new_row
        _step.__name__ = f"wrapped({step.__name__})"
        super().execute_row_step(_step)
