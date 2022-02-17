use pyo3::types::PyDict;
use pyo3::{PyResult, Python};

pub(crate) fn register_tradai_module(py: Python) -> PyResult<()> {
    use crate::__PYO3_PYMODULE_DEF_TRADAI;
    let m = wrap_pymodule!(tradai)(py);
    Python::with_gil(|py| {
        let dict = PyDict::new(py);
        dict.set_item("m", m)?;
        py.run(r#"import sys; sys.modules["tradai"] = m"#, None, Some(dict))?;
        Ok(())
    })
}
