use std::convert::From;
use std::fmt;
use std::sync::Arc;

use backtest::RecordBatch;
use pyo3::ffi::Py_uintptr_t;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::PyList;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ffi;
use arrow::ffi::Ffi_ArrowSchema;
use pyo3::exceptions::PyOSError;

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
        let c_schema = Ffi_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const Ffi_ArrowSchema;

        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;

        let field = unsafe { ffi::import_field_from_c(&c_schema).map_err(PyO3ArrowError::from)? };

        Ok(field.data_type)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let schema_ptr = Box::new(ffi::Ffi_ArrowSchema::empty());
        let schema_ptr = Box::into_raw(schema_ptr);

        unsafe {
            ffi::export_field_to_c(&Field::new("me", self.clone(), false), schema_ptr);
        };

        let pa = py.import("pyarrow")?;

        let dt = pa
            .getattr("DataType")?
            .call_method1("_import_from_c", (schema_ptr as Py_uintptr_t,))?;

        unsafe { Box::from_raw(schema_ptr) };

        Ok(dt.to_object(py))
    }
}

impl PyArrowConvert for Field {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = Ffi_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const Ffi_ArrowSchema;

        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;

        let field = unsafe { ffi::import_field_from_c(&c_schema).map_err(PyO3ArrowError::from)? };

        Ok(field)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let schema_ptr = Box::new(ffi::Ffi_ArrowSchema::empty());
        let schema_ptr = Box::into_raw(schema_ptr);

        unsafe {
            ffi::export_field_to_c(&self, schema_ptr);
        };

        let pa = py.import("pyarrow")?;

        let dt = pa
            .getattr("Field")?
            .call_method1("_import_from_c", (schema_ptr as Py_uintptr_t,))?;

        unsafe { Box::from_raw(schema_ptr) };

        Ok(dt.to_object(py))
    }
}

impl PyArrowConvert for Schema {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = Ffi_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const Ffi_ArrowSchema;

        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;

        let field = unsafe { ffi::import_field_from_c(&c_schema).map_err(PyO3ArrowError::from)? };
        if let DataType::Struct(fields) = field.data_type {
            Ok(Schema::from(fields))
        } else {
            Err(PyO3ArrowError::ArrowError(ArrowError::ExternalFormat(
                "Unable to interpret C data struct as a Schema".to_string(),
            ))
            .into())
        }
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let _dtype = DataType::Struct(self.fields.clone());
        let c_schema = Ffi_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const Ffi_ArrowSchema;

        // unsafe {
        //     ffi::export_field_to_c(&Field::new("f", dtype, false), c_schema);
        // }

        let module = py.import("pyarrow")?;
        let class = module.getattr("Schema")?;
        let schema = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(schema.into())
    }
}

impl PyArrowConvert for Arc<dyn Array> {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // prepare a pointer to receive the Array struct
        let array = Box::new(ffi::Ffi_ArrowArray::empty());
        let schema = Box::new(ffi::Ffi_ArrowSchema::empty());

        let array_ptr = &*array as *const ffi::Ffi_ArrowArray;
        let schema_ptr = &*schema as *const ffi::Ffi_ArrowSchema;

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1("_export_to_c", (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t))?;

        let field = unsafe { ffi::import_field_from_c(schema.as_ref()).map_err(PyO3ArrowError::from)? };
        let array = unsafe { ffi::import_array_from_c(array, &field).map_err(PyO3ArrowError::from)? };

        Ok(array.into())
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array_ptr = Box::new(ffi::Ffi_ArrowArray::empty());
        let schema_ptr = Box::new(ffi::Ffi_ArrowSchema::empty());

        let array_ptr = Box::into_raw(array_ptr);
        let schema_ptr = Box::into_raw(schema_ptr);

        unsafe {
            ffi::export_field_to_c(&Field::new("", self.data_type().clone(), true), schema_ptr);
            ffi::export_array_to_c(self.clone(), array_ptr);
        };

        let pa = py.import("pyarrow")?;

        let array = pa.getattr("Array")?.call_method1(
            "_import_from_c",
            (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
        )?;

        unsafe {
            Box::from_raw(array_ptr);
            Box::from_raw(schema_ptr);
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
            py_names.push(field.name.clone());
        }

        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatch")?;
        let record = class.call_method1("from_arrays", (py_arrays, py_names))?;

        Ok(PyObject::from(record))
    }
}
