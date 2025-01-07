from src.expectation.expectation import Expectation

class ValidTimeFormatExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)
        self.time_format = add_info.get("time_format", "")

    def test(self, ge_df):
        if not self.time_format:
            raise ValueError("Time format must be provided in add_info.")
        ge_df.expect_column_values_to_match_strftime_format(
            column=self.column,
            strftime_format=self.time_format,
            meta={"dimension": self.dimension}
        )
