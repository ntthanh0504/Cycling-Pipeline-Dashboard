from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from src.data_quality.data_quality import DataQuality
from src.utils.utils import create_df_from_dq_results


def get_violated_rows(pyspark_df, dq_results):
    violated_rows = pyspark_df
    for result in dq_results["results"]:
        if not result["success"]:
            # Lấy tên cột và danh sách giá trị vi phạm
            column_name = result["expectation_config"]["kwargs"]["column"]
            unexpected_values = result["result"]["partial_unexpected_list"]

            # Xử lý nếu có giá trị `null` trong danh sách vi phạm
            if None in unexpected_values or "null" in unexpected_values:
                # Lọc các dòng có giá trị null
                violated_rows = violated_rows.filter(F.col(column_name).isNull())
            else:
                # Lọc các dòng có giá trị không hợp lệ khác
                violated_rows = violated_rows.filter(F.col(column_name).isin(unexpected_values))
    
    return violated_rows


# Initialize Spark session
spark = SparkSession.builder.appName("DataQuality").getOrCreate()

# Load data
df = spark.read.csv("external_data/raw/2018 Q1 (Jan-Mar)-Central.csv", header=True)
print(df.show())

# Data quality check
dq = DataQuality(df, "src/config/test.json")
dq_results = dq.run_test()

# Convert dq results to DataFrame for display
dq_df = create_df_from_dq_results(spark, dq_results)
dq_df.show()

# Collect violations
violated_rows = get_violated_rows(df, dq_results)
violated_rows.show()

spark.stop()
