from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Airflow',
}
spark_config = {
    "spark.cores.max": "1",
    "spark.executor.memory": "1g",
}

with DAG(
        dag_id='simple_dag',
        default_args=default_args,
        start_date=days_ago(0)
) as dag:
    start = DummyOperator(task_id='start')

    amason_shows = SparkSubmitOperator(
        conf=spark_config,
        name="amason_shows",
        conn_id="spark_default",
        java_class="com.tvshow.TvShowAnalysis",
        task_id="spark_job_amason_shows",
        application="/usr/local/spark/app/TVShow.jar",
        application_args=[Variable.get("ams_app_name"), Variable.get("log_level"), Variable.get("ams_path")])

    dizney_shows = SparkSubmitOperator(
        conf=spark_config,
        name="dizney_shows",
        conn_id="spark_default",
        java_class="com.tvshow.TvShowAnalysis",
        task_id="spark_job_dizney_shows",
        application="/usr/local/spark/app/TVShow.jar",
        application_args=[Variable.get("diz_app_name"), Variable.get("log_level"), Variable.get("diz_path")])

    netflics_shows = SparkSubmitOperator(
        conf=spark_config,
        name="netflics_shows",
        conn_id="spark_default",
        java_class="com.tvshow.TvShowAnalysis",
        task_id="spark_job_netflics_shows",
        application="/usr/local/spark/app/TVShow.jar",
        application_args=[Variable.get("ntf_app_name"), Variable.get("log_level"), Variable.get("ntf_path")])

    finish = DummyOperator(task_id='finish')

    start >> [amason_shows, dizney_shows, netflics_shows] >> finish

