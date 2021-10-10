import datetime
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.utils.email import send_email
import pandas as pd
from google.cloud import bigquery
import pandas_gbq
import os
from cryptography.fernet import Fernet
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

gcp_key_route = Variable.get("gcp_key_route")
os.environ['FERNET_KEY'] = Fernet.generate_key().decode()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =gcp_key_route
client = bigquery.Client()
project_id = '<project_id>'

#PARAMETERS
STARTDATE =datetime.datetime.now() - datetime.timedelta(days=1)
EMAILTO = 'eunice@mail.io'

default_args = {
    'owner': 'Eunice',
    'depends_on_past': False,
    'email': [EMAILTO],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': STARTDATE
}

def notify_email(context):
        T= context['ti']
        ti = context['ti'].task
        subject = "Airflow Alert Dag:"+ ti.dag_id
        body =f"""
            <h3>Hi dev,</h3> <br>
            <h4>Error in task"""+ti.task_id+ """
            <br>
            Occur at:"""+datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"""</h4>
            <br><h6>Check to more info</h6>
        """
        send_email(EMAILTO, subject, body)

#BEGIN DAG


def insert_metadata(table):
    table_id = "<project_id>.data_metadata.tables_execution"
    rows_to_insert = [
        {u"table_schema":"data_landing"
        ,u"table_name": table.table_id
        ,u"count_rows":table.num_rows
        ,u"date_last_execution":str(table.modified - datetime.timedelta(hours=6, minutes=0)).split(".")[0]}
    ]
    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added in metadata.")
    else:
        print("Encountered errors while inserting rows in metadata: {}".format(errors))

def insert_table(query,table_id):
    job = client.query(query, location="US")
    job.result()
    table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
    return table

def check_table(table_name):
    sql = """
    SELECT count_rows
    FROM `<project_id>.data_metadata.tables_execution`
    WHERE table_name = '"""+table_name+"""'
    order by date_last_execution desc
    LIMIT 2
    """
    df = pandas_gbq.read_gbq(sql, project_id=project_id)
    print("Check: {}".format(table_name))
    print(df)

with airflow.DAG(
        'landing_bigquery',
        'catchup=False',
        default_args=default_args,
        on_failure_callback=notify_email,
        schedule_interval='@daily',
        )as dag:


    def load_table(**kwargs):
        table_id = "<project_id>.data_landing.table"
        query = "call data_landing.prc_table();"
        table = insert_table(query,table_id)
        insert_metadata(table)
        check_table(table.table_id)


    load_table = PythonOperator(
        task_id="load_table",
        python_callable=load_table,
        on_failure_callback=notify_email,
        provide_context=True

    )



    check_table = BigQueryCheckOperator(
    task_id = 'check_table',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM <project_id>.data_landing.table')



    trigger_dan_run_operator = TriggerDagRunOperator(
    task_id='trigger_task',
    trigger_dag_id='staging_bigquery')

(load_table>>check_table)>> trigger_dan_run_operator
