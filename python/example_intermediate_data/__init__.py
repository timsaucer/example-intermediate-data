from typing import List
from ._internal import MyDataStoreRaw
from datafusion.expr import Expr
import pyarrow as pa

class MyDataStore:
    def __init__(self, special_cases: List[str]) -> None:
        self.data_store = MyDataStoreRaw(special_cases)
        
    def initialize(self, string_vals: pa.Array, num_vals: pa.Array) -> pa.Array:
        return self.data_store.initialize(string_vals, num_vals)

    def replace_from_store(self, string_vals: pa.Array, num_vals: pa.Array) -> pa.Array:
        return self.data_store.replace_from_store(string_vals, num_vals)


