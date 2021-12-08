#### Trader Python Dynamic Library

This crate can be used to produce a single python package containing all the pyo3 generated python code as well as all the linked rust code in a dynamic library.

### Building the python dynamic library 

```maturin build -i $(python_target) --cargo-extra-args="--features=static"```

### Changing the python version for compilation 

set the following environment variable : ```PYO3_PYTHON=python3.9```

