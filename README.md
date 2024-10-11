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

[Top](#top)


## License

See the [LICENSE](LICENSE) file.

[Top](#top)


[about-marc]: https://en.wikipedia.org/wiki/MARC_standards
[pep-621]: https://peps.python.org/pep-0621/
[pyenv]: https://github.com/pyenv/pyenv
[pyenv-virtualenv]: https://github.com/pyenv/pyenv-virtualenv
[venv]: https://docs.python.org/3/library/venv.html
