from src.expectation.expectation import Expectation

class ValidDateFormatExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)
        self.date_format = add_info.get("date_format", "")

    def test(self, ge_df):
        if not self.date_format:
            raise ValueError("Date format must be provided in add_info.")
        ge_df.expect_column_values_to_match_strftime_format(
            column=self.column,
            strftime_format=self.date_format,
            meta={"dimension": self.dimension}
        )
