# Contributing

## Installation

Phaser is built with python 3.

To set up project for contributing:

```
% python -m venv venv
% source venv/bin/activate
% pip install -r dev_requirements.txt
% pip install -e .  # installs the phaser library (in edit mode!) so pytest can import it
```

Note that 'requirements.txt' is empty because phaser has no dependencies beyond the python language and its 
default libraries.  A project that uses pandas can use pandas DataFrames with phaser, but phaser itself does
not require pandas.  Pandas is required to test the features that use pandas, however, which is why pandas
is listed in dev_requirements.txt.

## Code of conduct

Please follow the [django code of conduct](https://www.djangoproject.com/conduct/).

## Coding guidelines

## How to test

```
% pytest
```

## How to document

We use Sphinx to create docs in the docs folder in MyST Markdown format.  When built, the docs also fold in 
docstrings from within the library's python modules.

```
% pip install -r docs/requirements.txt
% cd docs
% make html
% open _build/html/index.html

```

First time: I had to do `brew install sphinx-doc` to run make html 

## How to build/distribute

Edit pyproject.toml to update version number if appropriate, as well as the version in __init__.py.

```
% pip install twine
% pip install build
% python3 -m build
% python3 -m twine upload --repository testpypi dist/*
```

If that looks good, remove the testpypi repository and upload to pypi itself.