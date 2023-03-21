use std::convert::From;
use std::fmt;
use std::ptr::addr_of_mut;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ffi;
use arrow::ffi::ArrowArrayRef;
use pyo3::exceptions::PyOSError;
//use pyo3::ffi;
use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::PyList;

use backtest::RecordBatch;

/// an error that bridges ArrowError with a Python error
#[derive(Debug)]
enum PyO3ArrowError {
    ArrowError(ArrowError),
}

impl fmt::Display for PyO3ArrowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PyO3ArrowError::ArrowError(ref e) => e.fmt(f),
        }
    }
}

impl std::error::Error for PyO3ArrowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            // The cause is the underlying implementation error type. Is implicitly
            // cast to the trait object `&error::Error`. This works because the
            // underlying type already implements the `Error` trait.
            PyO3ArrowError::ArrowError(ref e) => Some(e),
        }
    }
}

impl From<ArrowError> for PyO3ArrowError {
    fn from(err: ArrowError) -> PyO3ArrowError { PyO3ArrowError::ArrowError(err) }
}

impl From<PyO3ArrowError> for PyErr {
    fn from(err: PyO3ArrowError) -> PyErr { PyOSError::new_err(err.to_string()) }
}

import_exception!(pyarrow, ArrowException);

pub trait PyArrowConvert: Sized {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self>;
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject>;
}

impl PyArrowConvert for DataType {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = ffi::FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const ffi::FFI_ArrowSchema;

        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let field = Field::try_from(&c_schema).map_err(|e| PyO3ArrowError::ArrowError(e))?;

        Ok(field.data_type().clone())
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        match ffi::FFI_ArrowSchema::try_from(self) {
            Ok(ffi_schema) => {
                let schema_ptr = Box::new(ffi_schema);
                let schema_ptr = Box::into_raw(schema_ptr);
                let pa = py.import("pyarrow")?;
                let dt = pa
                    .getattr("DataType")?
                    .call_method1("_import_from_c", (schema_ptr as Py_uintptr_t,))?;
                Ok(dt.to_object(py))
            }
            Err(e) => Err(PyO3ArrowError::ArrowError(e).into()),
        }
    }
}

impl PyArrowConvert for Field {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = ffi::FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const ffi::FFI_ArrowSchema;

        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;

        let field = Field::try_from(&c_schema).map_err(|e| PyO3ArrowError::ArrowError(e))?;

        Ok(field)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        match ffi::FFI_ArrowSchema::try_from(self) {
            Ok(ffi_schema) => {
                let schema_ptr = Box::new(ffi_schema);
                let schema_ptr = Box::into_raw(schema_ptr);
                let pa = py.import("pyarrow")?;
                let dt = pa
                    .getattr("Field")?
                    .call_method1("_import_from_c", (schema_ptr as Py_uintptr_t,))?;
                Ok(dt.to_object(py))
            }
            Err(e) => Err(PyO3ArrowError::ArrowError(e).into()),
        }
    }
}

impl PyArrowConvert for Schema {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = Box::new(ffi::FFI_ArrowSchema::empty());
        let c_schema_ptr = &*c_schema as *const ffi::FFI_ArrowSchema;

        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        match Schema::try_from(c_schema.as_ref()) {
            Ok(schema) => {
                let field = schema.field(0);
                if let DataType::Struct(_fields) = &field.data_type() {
                    Ok(schema)
                } else {
                    Err(PyO3ArrowError::ArrowError(ArrowError::SchemaError(
                        "Unable to interpret C data struct as a Schema".to_string(),
                    ))
                    .into())
                }
            }
            Err(e) => Err(PyO3ArrowError::ArrowError(e).into()),
        }
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let _dtype = DataType::Struct(self.fields.clone());
        let c_schema = ffi::FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const ffi::FFI_ArrowSchema;

        let module = py.import("pyarrow")?;
        let class = module.getattr("Schema")?;
        let schema = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(schema.into())
    }
}

impl PyArrowConvert for Arc<dyn Array> {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // prepare a pointer to receive the Array struct
        let mut array = ffi::FFI_ArrowArray::empty();
        let mut schema = ffi::FFI_ArrowSchema::empty();

        let array_ptr = addr_of_mut!(array);
        let schema_ptr = addr_of_mut!(schema);

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1("_export_to_c", (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t))?;

        let arrow_array = ffi::ArrowArray::new(array, schema);
        match arrow_array.to_data() {
            Ok(array_data) => return Ok(make_array(array_data)),
            Err(e) => Err(PyO3ArrowError::ArrowError(e).into()),
        }
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array_ptr = Box::new(ffi::FFI_ArrowArray::new(self.data()));
        let schema_ptr = ffi::FFI_ArrowSchema::try_from(self.data_type()).map_err(|e| PyO3ArrowError::ArrowError(e))?;
        let schema_ptr = Box::new(schema_ptr);

        let array_ptr = Box::into_raw(array_ptr);
        let schema_ptr = Box::into_raw(schema_ptr);

        let pa = py.import("pyarrow")?;
        let array = pa.getattr("Array")?.call_method1(
            "_import_from_c",
            (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
        )?;

        unsafe {
            let _ = Box::from_raw(array_ptr);
            let _ = Box::from_raw(schema_ptr);
        };

        Ok(array.to_object(py))
    }
}

impl PyArrowConvert for RecordBatch {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let schema = value.getattr("schema")?;
        let schema = Arc::new(Schema::from_pyarrow(schema)?);

        let arrays = value.getattr("columns")?.downcast::<PyList>()?;
        let arrays = arrays.iter().map(ArrayRef::from_pyarrow).collect::<PyResult<_>>()?;

        let batch = RecordBatch::try_new(schema, arrays).map_err(PyO3ArrowError::from)?;
        Ok(batch)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let mut py_arrays = vec![];
        let mut py_names = vec![];

        let schema = self.schema();
        let fields = schema.as_ref().fields.iter();
        let columns = self.columns().iter();

        for (array, field) in columns.zip(fields) {
            py_arrays.push(array.to_pyarrow(py)?);
            py_names.push(field.name().clone());
        }

        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatch")?;
        let record = class.call_method1("from_arrays", (py_arrays, py_names))?;

        Ok(PyObject::from(record))
    }
}
