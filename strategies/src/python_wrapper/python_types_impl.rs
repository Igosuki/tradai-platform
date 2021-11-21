use crate::types::InputEvent;
use pyo3::{IntoPy, PyObject, Python, ToPyObject};

impl ToPyObject for InputEvent {
    fn to_object(&self, py: Python) -> PyObject {
        match self {
            InputEvent::BookPosition(bp) => IntoPy::into_py(bp.clone(), py),
            //putEvent::BookPositions(bp) => python! {'bp},
            _ => unimplemented!(),
        }
    }
}
