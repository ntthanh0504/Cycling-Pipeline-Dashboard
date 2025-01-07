from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_df_from_dq_results(spark, dq_results):
    dq_data = []
    for result in dq_results["results"]:
        if result["success"] == True:
            status = 'PASSED'
        else:
            status = 'FAILED'

        # Xử lý giá trị unexpected_percent
        unexpected_percent = result["result"].get("unexpected_percent", 0.0)
        if unexpected_percent is None:
            unexpected_percent = 0.0

        dq_data.append((
            result["expectation_config"]["kwargs"]["column"],
            result["expectation_config"]["meta"]["dimension"],
            status,
            result["expectation_config"]["expectation_type"],
            result["result"].get("unexpected_count", 0),
            result["result"].get("element_count", 0),
            unexpected_percent,
            float(100 - unexpected_percent)
        ))

    dq_columns = [
        "column",
        "dimension",
        "status",
        "expectation_type",
        "unexpected_count",
        "element_count",
        "unexpected_percent",
        "percent"
    ]
    dq_df = spark.createDataFrame(data=dq_data, schema=dq_columns)
    return dq_df

def get_violated_rows(pyspark_df, dq_results):
    violated_df = []
    for result in dq_results["results"]:
        if not result["success"]:
            # Lấy tên cột và danh sách giá trị vi phạm
            column_name = result["expectation_config"]["kwargs"]["column"]
            unexpected_values = result["result"]["partial_unexpected_list"]

            # Xử lý nếu có giá trị `null` trong danh sách vi phạm
            if None in unexpected_values or "null" in unexpected_values:
                # Lọc các dòng có giá trị null
                violated_rows = pyspark_df.filter(F.col(column_name).isNull())
                violated_df.append(violated_rows)
            else:
                # Lọc các dòng có giá trị không hợp lệ khác
                violated_rows = pyspark_df.filter(F.col(column_name).isin(unexpected_values))
                violated_df.append(violated_rows)
    return violated_df