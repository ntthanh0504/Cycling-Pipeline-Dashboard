from src.expectation.expectation import Expectation

class GreaterThanOrEqualToExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)
        self.min_value = add_info.get("min_value")

    def test(self, ge_df):
        if self.min_value is None:
            raise ValueError("A minimum value (min_value) must be provided in add_info.")
        ge_df.expect_column_values_to_be_between(
            column=self.column,
            min_value=self.min_value,
            meta={"dimension": self.dimension}
        )
