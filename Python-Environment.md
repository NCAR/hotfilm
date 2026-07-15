# Python Environment

The Python hot film processing code resides mostly in the [hotfilm](hotfilm)
package directory.  There is a [pyproject.toml](pyproject.toml) file which
describes the Python project and lists the Python package dependencies.

The default dependencies only include what is needed to run the hot film
processing scripts, to generate NetCDF files of hot film voltages and derive
calibrated hot film wind speeds.  The extra dependency `dev` includes
development tools, currently `pytest`.  The extra dependency `web` is for the
web application which can plot raw hot film data.  That is mostly only useful
during data acquisition, as described in
[Data-Acquisition.md](docs/Data-Acquisition.md).

There are a few ways to install the dependencies to a Python environment. The
examples here use [uv](https://docs.astral.sh/uv/), but the commands for `pip`
and `pipenv` should be similar.

## Installing dependencies

After creating the virtual environment in `.venv` with `uv venv`, use
the `pip install` command below to install the default dependencies
according to the `requirements.txt` lock file.

```
uv venv
uv pip install -r requirements.txt
```

Development dependencies can be added by installing the `dev` group from
`pyproject.toml`:

```
uv pip install -r pyproject.toml --extra 'dev'
```

## Upgrading dependencies

This is one way to upgrade all the dependencies and generate a new lock file.

To install the latest versions of all package dependencies, install into a
fresh virtual environment and specify the `pyproject.toml` file in place of
`requirements.txt`:

```
uv venv
uv pip install -r pyproject.toml --extra 'dev'
```

Then update the `requirements.txt` lock file:

```
uv export --format requirements.txt > requirements.txt
```

## Viewing dependencies

It can be helpful to see the entire package dependency tree to understand why
certain packages are installed.

```
uv tree
```

The `pipdeptree` utility is another option:

```
uv pip install pipdeptree
uv run pipdeptree
```
