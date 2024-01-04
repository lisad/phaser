import pytest
from phaser import Column

def test_null_forbidden_but_null_default():
    with pytest.raises(Exception):
        Column(name='bogus', null=False, default='homer')

def test_required_values():
    mycol = Column('passenger', allowed_values=["Gilligan", "Skipper", "Professor"])
    mycol.check(["passenger"], [{"passenger": "Gilligan"}, {"passenger": "Skipper"}])
    with pytest.raises(Exception):
        mycol.check(["passenger"], [{"passenger": "Shakespeare"}])

def test_default_value():
    col = Column('location', default='HQ')
    data = [{'location': 'Atlanta'}, {'location': None}]
    col.check(['location'], data)
    for row in data:
        if row[col.name] is None:
            assert col.fix_value(row[col.name]) == 'HQ'
        else:
            assert col.fix_value(row[col.name]) == row[col.name]

def test_fix_value_fn_instance_method():
    col = Column('location', fix_value_fn='lstrip')
    assert col.fix_value('  Toronto') == 'Toronto'

def test_abs_on_int_column():
    #LMDTODO rewrite to actually use int column and include transform to int
    col = Column('value', fix_value_fn='abs')
    assert col.fix_value(-1) == 1

def test_fix_value_fn_value_as_param():
    col = Column('keycode', fix_value_fn='bytearray')
    assert col.fix_value([1, 2, 200]) == bytearray(b'\x01\x02\xc8')

def test_callable():
    def my_func(string):
        return string.strip().capitalize()

    col = Column('team', fix_value_fn=my_func)
    assert col.fix_value("  RAPTORS  ") == "Raptors"

def test_multiple_functions():
    col = Column('status', fix_value_fn=['lstrip', 'capitalize'])
    assert col.fix_value("  ACTIVE  ") == "Active  "
