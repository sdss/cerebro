# cerebro

![Versions](https://img.shields.io/badge/python->3.7-blue)
[![Documentation Status](https://readthedocs.org/projects/sdss-cerebro/badge/?version=latest)](https://sdss-cerebro.readthedocs.io/en/latest/?badge=latest)
[![Test Status](https://github.com/albireox/cerebro/workflows/Test/badge.svg)](https://github.com/sdss/sdss/actions)

A library to gather time-series data from different sources and store them, with focus on InfluxDB databases. Documentation and concepts are defined [here](https://sdss-cerebro.readthedocs.io/).

## Installation

In general you should be able to install `cerebro` by doing

```console
pip install sdss-cerebro
```

To build from source, use

```console
git clone git@github.com:sdss/cerebro
cd cerebro
pip install .
```

## Use

`cerebro` is meant to run as a daemon. The simplest way to run it is simply

```console
cerebro start
```

This will run all the sources and use all the observers. You can define a specific profile to use

```console
cerebro --profile lvm-lab start
```

or a series of sources

```console
cerebro --sources lvm_govee_clean_room,lvm_sens4_r1 start
```

Normally `cerebro` will run in detached/daemon mode. It's also possible to pass the flag `--debug` (`cerebro start --debug`) to run the code in the foreground.

Run `cerebro --help` to get all the options available.

## Development

`cerebro` uses [poetry](http://poetry.eustace.io/) for dependency management and packaging. To work with an editable install it's recommended that you setup `poetry` and install `cerebro` in a virtual environment by doing

```console
poetry install
```

Pip does not support editable installs with PEP-517 yet. That means that running `pip install -e .` will fail because `poetry` doesn't use a `setup.py` file. As a workaround, you can use the `create_setup.py` file to generate a temporary `setup.py` file. To install `cerebro` in editable mode without `poetry`, do

```console
pip install poetry
python create_setup.py
pip install -e .
```

The style code is [black](https://black.readthedocs.io/en/stable/).
