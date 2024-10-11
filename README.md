filtermarc
==========

* [About](#about)
* [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [License](#license)


## About

*`filtermarc`* is a Python package for filtering, selecting, and then
formatting large sets of [MARC][about-marc] records based on data content. This
is useful for efficiently creating custom data sets from very large sets of
MARC records, such as the ones from the Library of Congress.

[Top](#top)


## Installation

Filtermarc is in a prerelease state. Please see [Contributing](#contributing)
for the recommended development installation process.

[Top](#top)


## Usage

TODO

[Top](#top)


## Contributing

### Installing for Development and Testing

If you have write access to the repository, clone the project directly.

```bash
git clone https://github.com/llm4cat/filtermarc.git
```

Or, if you do not have write access to the repository, fork the project first
and then clone your fork.

```bash
git clone https://github.com/your-github-account/filtermarc.git
```

Create and activate a new virtual environment for development using whatever
method you prefer.

- I highly recommend using [pyenv] for managing multiple python versions.
- In conjunction with pyenv, you can use [venv] or [pyenv-virtualenv] for
  managing virtual environments.

From the filtermarc project root directory, you can install the package into
your development environment as an editable project with:

```bash
python -m pip install -e .[dev]
```

All dependency and build information is defined in `pyproject.toml` and follows
[PEP 621][pep-621]. Specifying `[dev]` ensures it includes the optional
development dependencies, namely `pytest`.

### Running Tests

Run the full test suite in your active environment by invoking

```bash
pytest
```

from the project root directory.

#### Tox

Use [tox] to do the following:

- Run linters and mypy checks.
- Run tests against oldest and latest available dependencies for all supported
  Python versions.
- Run steps to build the package and test the build.

Tox configuration is in the pyproject.toml file, under the [tool.tox] table.
This defines several environments: flake8, pylint, and each of py39 through
py312 using both the oldest possible dependencies and newest possible
dependencies. When you run tox, you can target a specific environment, a
specific list of environments, or all of them.

When tox runs, it automatically builds each virtual environment it needs, and
then it runs whatever commands it needs within that environment (for linting,
or testing, etc.). All you have to do is expose all the necessary Python
binaries on the path, and tox will pick the correct one. My preferred way to
manage this is with [pyenv] + [pyenv-virtualenv].

For example:

1. Install pyenv and pyenv-virtualenv, if you haven't already.
2. Use pyenv to install the latest versions of Python 3.9 through 3.12.
3. Create an environment with tox installed.

    ```
    pyenv virtualenv 3.11.10 tox-3.11.10
    pyenv activate
    python -m pip install tox
    ```

4. In the project root, create a file called `.python-version`. Add all of the
   Python versions that you want tox to access, using the tox environment
   you created in the last step as your environment for that version. This
   should look something like this.

    ```
    3.9.20
    3.10.15
    tox-3.11.10
    3.12.6
    ```

5. At this point, `tox-3.11.10` is probably still activated. Issue a `pyenv
   deactivate` command so that pyenv picks up what's in the file.

6. All four environments are now active at once in that directory. When you run
   tox, the tox in your tox-3.11.10 environment will run, and it will pick up
   the appropriate binaries automatically (python3.9 through python3.12) since
   they're all on the PATH via pyenv's shim.

Invoke tox as needed to run linters and tests.

```bash
# Run default commands (linters and tests against all environments):
tox

# Run linters:
tox -e flake8,pylint_critical

# Run tests against specific environments:
tox -e py39-oldest,py39-newest
```

[Top](#top)


## License

See the [LICENSE](LICENSE) file.

[Top](#top)


[about-marc]: https://en.wikipedia.org/wiki/MARC_standards
[pep-621]: https://peps.python.org/pep-0621/
[pyenv]: https://github.com/pyenv/pyenv
[pyenv-virtualenv]: https://github.com/pyenv/pyenv-virtualenv
[tox]: https://tox.wiki/
[venv]: https://docs.python.org/3/library/venv.html
