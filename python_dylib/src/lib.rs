use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "strategies")]
fn strategies(py: pyo3::Python, m: &pyo3::types::PyModule) -> pyo3::PyResult<()> {
    ::strategies::python_strat::strat(py, m)?;
    Ok(())
}
