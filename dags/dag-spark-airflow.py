import datetime
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# Set up variables for the DAG
MAIN_CLASS= 'org.apache.spark.examples.JavaWordCount'
FILE_JAR=[models.Variable.get('gcp_bucket') + '/spark-examples_2.12-3.4.1.jar']
ARGUMENTS=[models.Variable.get('gcp_bucket') + '/text.txt']

default_dag_args = {
    'project_id': models.Variable.get('gcp_project'),
    'retry_delay': datetime.timedelta(minutes=5),
    'retries': 1
}

with models.DAG(
        'dag-spark-airflow',
        start_date=datetime.datetime(2023, 9, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_dag_args
) as dag:

    # Create Dataproc Cluster
    create_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_cluster',
        cluster_name='temp-spark-cluster-{{ ds_nodash }}',
        num_workers=2,
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
        image_version='2.0',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2',
        master_disk_size=50,
        worker_disk_size=50)

    # Run Spark Job
    run_count_word = dataproc_operator.DataProcSparkOperator(
        task_id='run_count_word',
        region=models.Variable.get('gce_region'),
        dataproc_jars=FILE_JAR,
        main_class=MAIN_CLASS,
        cluster_name='temp-spark-cluster-{{ ds_nodash }}',
        arguments=ARGUMENTS)

    # Delete Dataproc Cluster
    delete_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        region=models.Variable.get('gce_region'),
        cluster_name='temp-spark-cluster-{{ ds_nodash }}',
        # If trigger_rule all done, delete cluster and you can go home :)
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_cluster >> run_count_word >> delete_cluster