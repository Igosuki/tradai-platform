#### Trader Python Dynamic Library

This crate can be used to produce a single python package containing all the pyo3 generated python code as well as all
the linked rust code in a dynamic library.

### Building the python dynamic library

```maturin build -i $(python_target) --cargo-extra-args="--features=static"```

### Changing the python version for compilation

set the following environment variable : ```PYO3_PYTHON=python3.11```

### Disclaimer : the Python GIL

Pyo3 currently does not support new GIL per thread : https://github.com/PyO3/pyo3/issues/576

Therefore, it is critical that a single process runs only code owned by the runner, for instance for security, two users
should not be able to access strategies from other users on the same machine.

### Requirements

Using the python crate requires installing protoc as of now because of datafusion-substrait
