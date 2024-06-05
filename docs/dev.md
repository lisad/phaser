# Contributing

## Installation

Phaser is built with python 3.

To set up project for contributing:

```
% python -m venv venv
% source venv/bin/activate
% pip install -r requirements.txt
% pip install -e .  # installs the phaser library (in edit mode!) so pytest can import it
```

## Code of conduct

## Coding guidelines

## How to test

```
% pytest
```

## How to document

We use Sphinx to create docs in the docs folder in MyST Markdown format.  When built, the docs also fold in 
docstrings from within the library's python modules.

```
% cd docs
% make html
% open _build/html/index.html

```