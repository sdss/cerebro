# cerebro

![Versions](https://img.shields.io/badge/python->3.7-blue)
[![Documentation Status](https://readthedocs.org/projects/sdss-cerebro/badge/?version=latest)](https://sdss-cerebro.readthedocs.io/en/latest/?badge=latest)
[![Test Status](https://github.com/sdss/cerebro/workflows/Test/badge.svg)](https://github.com/sdss/cerebro/actions)
[![codecov](https://codecov.io/gh/sdss/cerebro/branch/master/graph/badge.svg)](https://codecov.io/gh/sdss/cerebro)

A library to gather time-series data from different subsystems and store them in an InfluxDB server.

## Installation

In general you should be able to install ``cerebro`` by doing

```console
pip install sdss-cerebro
```

To build from source, use

```console
git clone git@github.com:sdss/cerebro
cd cerebro
pip install .
```

## Development

`cerebro` uses [poetry](http://poetry.eustace.io/) for dependency management and packaging. To work with an editable install it's recommended that you setup `poetry` and install `cerebro` in a virtual environment by doing

```console
poetry install
```

Pip does not support editable installs with PEP-517 yet. That means that running `pip install -e .` will fail because `poetry` doesn't use a `setup.py` file. As a workaround, you can use the `create_setup.py` file to generate a temporary `setup.py` file. To install `cerebro` in editable mode without `poetry`, do

```console
pip install --pre poetry
python create_setup.py
pip install -e .[docs]
```
