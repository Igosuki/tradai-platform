use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "strategy")]
fn strategy(py: pyo3::Python, m: &pyo3::types::PyModule) -> pyo3::PyResult<()> {
    ::strategy::python_strat::strat(py, m)?;
    Ok(())
}
