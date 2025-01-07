from src.reader.JSONFileReader import JSONFileReader
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from src.expectation.not_null_expectation import NotNullExpectation
from src.expectation.unique_expectation import UniqueExpectation
from src.expectation.values_in_list_expectation import ValuesInListExpectation
from src.expectation.matches_regex_expectation import MatchesRegexExpectation
from src.expectation.valid_date_format_expectation import ValidDateFormatExpectation
from src.expectation.valid_time_format_expectation import ValidTimeFormatExpectation
from src.expectation.greater_or_equal_expectation import GreaterThanOrEqualToExpectation


class DataQuality:

    def __init__(self, pyspark_df, config_path):
        self.pyspark_df = pyspark_df
        self.config_path = config_path

    def rule_mapping(self, dq_rule):
        return {
            "check_if_not_null": "NotNullExpectation",
            "check_if_unique": "UniqueExpectation",
            "check_if_values_in_list": "ValuesInListExpectation",
            "check_if_matches_regex": "MatchesRegexExpectation",
            "check_if_valid_date_format": "ValidDateFormatExpectation",
            "check_if_valid_time_format": "ValidTimeFormatExpectation",
            "check_if_greater_than_or_equal_to": "GreaterThanOrEqualToExpectation",
        }[dq_rule]

    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)

    def convert_to_ge_df(self):
        return SparkDFDataset(self.pyspark_df)

    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()

    def run_test(self):
        ge_df = self.convert_to_ge_df()
        config = self.read_config()
        for column in config["columns"]:
            if column["dq_rule(s)"] is None:
                continue
            for dq_rule in column["dq_rule(s)"]:
                expectation_obj = globals()[self.rule_mapping(dq_rule["rule_name"])]
                expectation_instance = expectation_obj(
                    column["column_name"],
                    dq_rule["rule_dimension"],
                    dq_rule["add_info"],
                )
                expectation_instance.test(ge_df)

        dq_results = ge_df.validate()
        return dq_results
