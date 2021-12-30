use pyo3::types::PyDict;
use pyo3::{PyResult, Python};

pub(crate) fn register_strat_module(py: Python) -> PyResult<()> {
    use crate::PyInit_strategy;
    let m = wrap_pymodule!(strategy)(py);
    Python::with_gil(|py| {
        let dict = PyDict::new(py);
        dict.set_item("m", m)?;
        py.run(r#"import sys; sys.modules["strategy"] = m"#, None, Some(dict))?;
        Ok(())
    })
}
