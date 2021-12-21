use pyo3::types::PyDict;
use pyo3::{PyResult, Python};

pub(crate) fn register_strat(py: Python) -> PyResult<()> {
    use crate::PyInit_strat;
    let m = wrap_pymodule!(strat)(py);
    Python::with_gil(|py| {
        let dict = PyDict::new(py);
        dict.set_item("m", m)?;
        py.run(r#"import sys; sys.modules["strategy"] = m"#, None, Some(dict))?;
        Ok(())
    })
}
