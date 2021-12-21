#[macro_use]
extern crate pyo3;

#[pymodule]
#[pyo3(name = "strategy")]
fn strategy(py: pyo3::Python, m: &pyo3::types::PyModule) -> pyo3::PyResult<()> {
    strategy_python::strat(py, m)?;
    Ok(())
}
