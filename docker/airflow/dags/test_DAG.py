import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
        dag_id = "spark_airflow_dag",
        default_args=default_args,
        schedule_interval=None,	
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

Extract = SparkSubmitOperator(
    task_id="spark_submit_task",
    application="/opt/airflow/dags/src/spark/mongo_demo.py",
    conn_id="spark_local",
    # jars="/opt/airflow/shared-jars/delta-core_2.12-2.4.0.jar,/opt/airflow/shared-jars/delta-spark_2.12-3.2.0.jar,/opt/airflow/shared-jars/delta-storage-3.2.0.jar",
    # #     /opt/airflow/shared-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/airflow/shared-jars/kafka-clients-3.4.0.jar",
    packages="org.mongodb.spark:mongo-spark-connector_2.12:10.1.1, org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.kafka:kafka-clients:3.3.1",
    verbose=True,
    dag=spark_dag
)


Extract