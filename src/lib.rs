use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayData, BooleanArray, PrimitiveArray, StringArray, UInt64Array};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::UInt64Type;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::types::PyModuleMethods;
use pyo3::Py;
use pyo3::{pyclass, pymethods, pymodule, types::PyModule, Bound, PyAny, PyResult, Python};

#[pyclass]
struct MyDataStoreRaw {
    replacements: HashMap<String, u64>,
    special_cases: HashSet<String>,
}

#[pymethods]
impl MyDataStoreRaw {
    #[new]
    fn new(special_cases: Vec<String>) -> Self {
        let special_cases = special_cases.into_iter().collect();

        Self {
            replacements: HashMap::default(),
            special_cases,
        }
    }

    fn initialize(
        &mut self,
        py: Python<'_>,
        expr_a: &Bound<'_, PyAny>,
        expr_b: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let a_arr: StringArray = ArrayData::from_pyarrow_bound(expr_a)?.into();
        let b_arr: PrimitiveArray<UInt64Type> = ArrayData::from_pyarrow_bound(expr_b)?.into();

        let values = a_arr
            .iter()
            .zip(b_arr.values().iter())
            .map(|(a_val, b_val)| {
                // Here is where we actually do the initalization of `replacements`

                let ret = match a_val {
                    Some(a) => self.special_cases.contains(a),
                    None => false,
                };

                if !ret {
                    // a_val is already checked
                    let max_val = self
                        .replacements
                        .entry(a_val.unwrap().to_string())
                        .or_default();
                    *max_val = (*max_val).max(*b_val);
                }

                ret
            });

        // This is a bit of a hack - we want to make sure the optimizer
        // doesn't remove this operation, so pass some dummy array data
        // back.
        let res: BooleanArray = BooleanBuffer::from_iter(values).into();

        res.into_data().to_pyarrow(py)
    }

    fn replace_from_store(
        &self,
        py: Python<'_>,
        expr_a: &Bound<'_, PyAny>,
        expr_b: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let a_arr: StringArray = ArrayData::from_pyarrow_bound(expr_a)?.into();
        let b_arr: PrimitiveArray<UInt64Type> = ArrayData::from_pyarrow_bound(expr_b)?.into();

        let values = a_arr
            .iter()
            .zip(b_arr.values().iter())
            .map(|(a_val, b_val)| {
                a_val
                    .map(|a| match self.replacements.get(a) {
                        Some(b_replacement) => b_replacement,
                        None => b_val,
                    })
                    .cloned()
                    .unwrap_or(0)
            });

        let res = UInt64Array::from_iter_values(values);

        res.into_data().to_pyarrow(py)
    }
}

#[pymodule]
fn _internal(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<MyDataStoreRaw>()?;

    Ok(())
}
