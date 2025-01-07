from src.expectation.expectation import Expectation

class MatchesRegexExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)
        self.regex_pattern = add_info.get("regex_pattern", "")

    def test(self, ge_df):
        if not self.regex_pattern:
            raise ValueError("Regex pattern must be provided in add_info.")
        ge_df.expect_column_values_to_match_regex(
            column=self.column,
            regex=self.regex_pattern,
            meta={"dimension": self.dimension}
        )
