from airflow.hooks.base_hook import BaseHook
import airflow
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import datetime
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import os
from airflow.models import Variable

gcp_key_route = Variable.get("gcp_key_route")
SLACK_CONN_ID = 'slack'
STARTDATE =datetime.datetime.now() - datetime.timedelta(days=1)
EMAILTO = 'eunice@mail.io'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_key_route
client = bigquery.Client()
project_id = '<project_id>'

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

def resolve_id():
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg=":red_circle: Hello."
    query = """
    UPDATE `<PROJECT_ID>.<schema>.<table>`A
    SET A.id = B.id
    FROM `<PROJECT_ID>.<schema>.<table>` B
    WHERE  A.title = B.title
    AND  A.audiobook_id is null;
    """
    job = client.query(query, location="US")
    print(job.result())
    query = """
    SELECT audio_contents_test.title FROM `<PROJECT_ID>.<schema>.<table>` A
    LEFT JOIN `<PROJECT_ID>.<schema>.<table>` B
    ON  A.id = B.id
    WHERE  A.id is null;
    """
    df = pandas_gbq.read_gbq(query, project_id=project_id)
    print(df['title'].astype(str))
    titulos = ""
    for index,row in df.iterrows():
        titulos += "["+str(index+1)+"] "+str(row['title'])+"\n"
    if len(df)>0:
         msj =  "*Reporte Semanal*: ¡Hola team! Los siguientes campos aún no tienen id automático: \n {} _Puedes revisar su información en la base de datos_.".format(titulos)
    else:
         msj = "*Reporte Semanal*: El proceso automático de ingestar audiobook_id fue un éxito :smile:"
    return msj

with airflow.DAG(
        'airflow_alarms',
        'catchup=False',
        default_args=default_args,
        on_failure_callback=notify_email,
        schedule_interval='@weekly',
        )as dag:

        resolve_id = SlackWebhookOperator(
        task_id='resolve_id',
        http_conn_id='slack',
        webhook_token= BaseHook.get_connection(SLACK_CONN_ID).password,
        message=resolve_audiobook()
        )

        resolve_audiobook
