import requests
import datetime
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os
from airflow.models import Variable
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import airflow
from airflow.operators.python_operator import PythonOperator

gcp_key_route = Variable.get("gcp_key_route")
airtable_key = Variable.get("airtable_key")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_key_route
client = bigquery.Client()
project_id = 'aerobic-datum-206523'
STARTDATE = datetime.datetime.now() - datetime.timedelta(days=1)


default_args = {
    'owner': 'Eunice',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': STARTDATE
}

def notify_email(**kwargs):
    message = Mail(
    from_email='eunice@mail.io',
    to_emails='eunice@mail.io',
    subject="Airflow Alert in DW step.",
    html_content=f"""
                 <h3>Hi dev,</h3> <br>
                 <h4>Error in step data_dw
                 <br>
                 Occur at:"""+datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")+"""</h4>
                 <br><h6>Check to more info</h6>
             """)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)
def strmerge_contracts_id(strcolumn,df_contracts_id):
    for i in df_contracts_id['Libros']:
        if strcolumn in i:

            return df_contracts_id[df_contracts_id['Libros'] == i]['contracts_id'].values[0]
            break
        else:
            pass
def get_airtable_libros():
    url = "https://api.airtable.com/v0/<id>/Libros?api_key="+airtable_key
    res = requests.get(url)
    json_libros = res.json()
    json_data = []
    for record in json_libros['records']:
        json_data.append(
            [record['fields']['Título del libro'],
            record['fields']['Subtítulo']if('Subtítulo' in record['fields'])else '',
            record['fields']['Serie']if('Serie' in record['fields'])else '',
            record['fields']['ISBN']if('ISBN' in record['fields'])else '',
            record['fields']['Created']if('Created' in record['fields'])else ''
            ]
         )

    df_airtable_libros = pd.DataFrame.from_records( json_data )
    df_airtable_libros.columns = [
                                        'Título del libro',
                                        'Subtítulo',
                                        'Serie',
                                        'ISBN',
                                        'Created'
                                        ]
    return df_airtable_libros
def get_airtable_contratos():
    url = "https://api.airtable.com/v0/<id>/Contratos?api_key="+airtable_key
    res = requests.get(url)
    json_contratos = res.json()
    json_data = []
    for record in json_contratos['records']:
        json_data.append(
            [record['fields']['Nombre del proyecto'],
            record['fields']['Creador']if('Creador' in record['fields'])else '',
            record['fields']['Firma del contrato']if('Firma del contrato' in record['fields'])else '',
            record['fields']['Regalías']if('Regalías' in record['fields'])else '',
            record['fields']['Anticipo']if('Anticipo' in record['fields'])else '',
            record['fields']['Vencimiento del contrato']if('Vencimiento del contrato' in record['fields'])else '',
            record['fields']['Libros']if('Libros' in record['fields'])else ''
            ]
         )

    df_airtable_contratos = pd.DataFrame.from_records( json_data )
    df_airtable_contratos.columns = [
                                    'Nombre del proyecto',
                                    'Creador',
                                    'Firma del contrato',
                                    'Regalías',
                                    'Anticipo',
                                    'Vencimiento del contrato',
                                    'Libros'
                                    ]
    return df_airtable_contratos

def get_airtable_creadores():
    url = "https://api.airtable.com/v0/<id>/Creadores?api_key="+airtable_key
    res = requests.get(url)
    json_creadores = res.json()
    json_data = []
    for record in json_creadores['records']:
        json_data.append(
            [record['fields']['Nombre del creador'],
            record['fields']['Correo electrónico']if('Correo electrónico' in record['fields'])else '',
            record['fields']['Celular']if('Celular' in record['fields'])else '',
            record['fields']['Categoría']if('Categoría' in record['fields'])else '',
            record['fields']['Dirección']if('Dirección' in record['fields'])else '',
            record['fields']['Firma del contrato']if('Firma del contrato' in record['fields'])else '',
            record['fields']['Regalías']if('Regalías' in record['fields'])else '',
            record['fields']['Anticipo']if('Anticipo' in record['fields'])else '',
            record['fields']['Vencimiento del contrato']if('Vencimiento del contrato' in record['fields'])else ''
            ]
         )

    df_airtable_creadores = pd.DataFrame.from_records( json_data )
    df_airtable_creadores.columns = [
                                    'Nombre del creador',
                                    'Correo electrónico',
                                    'Celular',
                                    'Categoría',
                                    'Dirección',
                                    'Firma del contrato',
                                    'Regalías',
                                    'Anticipo',
                                    'Vencimiento del contrato'
                                    ]
    return df_airtable_creadores

def insert_into_book_contents(df):
    job_config = bigquery.LoadJobConfig(
     schema=[
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sub_title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("isbn", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("book_content_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("contracts_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("book_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("updated_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    call_bigquery_job(df,'book_contents_test', job_config)

def get_bigquery(table):
    sql = """
    SELECT *
    FROM `{}.content_core_db.{}`
    """.format(project_id,table)
    df_bigquery = pd.read_gbq(sql, project_id=project_id)
    return df_bigquery
def call_bigquery_job(df_to_insert,table, job_config):
    job = client.load_table_from_dataframe(
        df_to_insert, project_id+'.content_core_db.'+table, job_config=job_config
    )
    job.result()

with airflow.DAG(
        'airtable_bigquery',
        'catchup=False',
        default_args=default_args,
        on_failure_callback=notify_email,
        schedule_interval=None,

        )as dag:

        def set_contents(**kwargs):
            #get source bigquery and airtable api
            df_bigquery_book_contents = get_bigquery('book_contents_test')
            df_airtable_libros = get_airtable_libros()
            #merge outer to get new records
            df_libros = df_bigquery_book_contents.merge(df_airtable_libros, left_on="title", right_on= "Título del libro",how="outer",indicator=True)
            df_libros = df_libros[df_libros['_merge']=='right_only']
            #get only fields to use and generate id
            df_libros = df_libros[['Título del libro','Subtítulo','Created','Serie','ISBN']]
            df_libros = df_libros.reset_index()
            df_libros = df_libros.rename(columns={"index":"book_content_id"})
            ids_bigquery_book_content = df_bigquery_book_contents["book_content_id"]
            max_index = ids_bigquery_book_content.max()
            df_libros['book_content_id'] = df_libros.index + max_index
            #set other fields calculated
            df_libros['contracts_id'] = None #pending
            df_libros['book_id'] = None
            df_libros['content_type'] = np.where(df_libros['Serie']!='', 'Serie', 'bookbook')
            df_libros['updated_at'] = datetime.datetime.now()
            df_libros['created_at'] = pd.to_datetime(df_libros['Created'], format='%Y-%m-%d', errors='ignore')
            #change order and rename fields to insert in bigquery
            df_to_insert_book_contents = df_libros[['book_content_id', 'contracts_id','Título del libro','Subtítulo','content_type','ISBN','book_id','created_at','updated_at']]
            df_to_insert_book_contents = df_to_insert_book_contents.rename(columns={"Título del libro": "title", "Subtítulo": "sub_title","ISBN":'isbn'})
            #the table in bigquery will truncate so union old and new records
            df_to_insert_book_contents = pd.concat([df_bigquery_book_contents, df_to_insert_book_contents])
            #control NAN fields
            df_to_insert_book_contents['book_id'] = df_to_insert_book_contents['book_id'].fillna(np.nan).replace([np.nan], [None])
            df_to_insert_book_contents['isbn'] = df_to_insert_book_contents['isbn'].fillna(np.nan).replace([np.nan], [None])
            df_to_insert_book_contents['sub_title'] = df_to_insert_book_contents['sub_title'].fillna(np.nan).replace([np.nan], [None])
            #set job to insert in bigquery
            insert_into_book_contents(df_to_insert_book_contents)


        def set_creators(**kwargs):
            df_bigquery_creators = get_bigquery('creators_test')
            df_airtable_creadores = get_airtable_creadores()

            df_creadores = df_bigquery_creators.merge(df_airtable_creadores, left_on="full_name", right_on= "Nombre del creador",how="outer",indicator=True)
            df_creadores = df_creadores[df_creadores['_merge']=='right_only']
            df_creadores = df_creadores[['Nombre del creador','Dirección','Categoría','Correo electrónico','Celular']]
            df_creadores = df_creadores.reset_index()
            df_creadores = df_creadores.rename(columns={"index":"creator_id"})
            ids_bigquery_creator = df_bigquery_creators["creator_id"]
            max_index = ids_bigquery_creator.max()
            df_creadores['creator_id'] = df_creadores.index + max_index
            df_creadores['stage_name'] = None
            df_to_insert_creators = df_creadores[['creator_id', 'Dirección','Categoría','Nombre del creador','Correo electrónico','Celular','stage_name']]
            df_to_insert_creators = df_to_insert_creators.rename(columns={"Dirección": "direction", "Categoría": "category","Nombre del creador":"full_name","Correo electrónico":"email", "Celular":"phone_number"})
            df_to_insert_creators = pd.concat([df_bigquery_creators, df_to_insert_creators])
            job_config = bigquery.LoadJobConfig(
             schema=[
                    bigquery.SchemaField("creator_id", bigquery.enums.SqlTypeNames.INT64),
                    bigquery.SchemaField("direction", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("category", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("full_name", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("phone_number", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("stage_name", bigquery.enums.SqlTypeNames.STRING),
                ],
                write_disposition="WRITE_TRUNCATE",
            )
            call_bigquery_job(df_to_insert_creators,'creators_test', job_config)



        def set_contracts(**kwargs):
            df_bigquery_contracts = get_bigquery('contracts_test')
            df_airtable_contratos = get_airtable_contratos()
            df_contratos = df_bigquery_contracts.merge(df_airtable_contratos, left_on="project_name", right_on= "Nombre del proyecto",how="outer",indicator=True)
            df_contratos = df_contratos[df_contratos['_merge']=='right_only']
            df_contratos = df_contratos[['Nombre del proyecto','Firma del contrato','Regalías','Vencimiento del contrato']]
            #agrupar y sumar royalties para obtener el general
            df_general_royalties = df_contratos[['Nombre del proyecto','Regalías']].groupby(['Nombre del proyecto']).sum()
            df_general_royalties = df_general_royalties.rename(columns={'Regalías':"sum_regalías"})
            df_contratos  = df_contratos.merge(df_general_royalties, left_on="Nombre del proyecto", right_on= "Nombre del proyecto",how="inner")
            #elegir el campo sum_royalties
            df_contratos = df_contratos[['Nombre del proyecto','Firma del contrato','sum_regalías','Vencimiento del contrato']]
            #eliminar los duplicados debido a los diferentes creadores que tiene un contrado
            df_contratos = df_contratos.drop_duplicates()
            #generación de id
            df_contratos = df_contratos.reset_index()
            df_contratos = df_contratos.rename(columns={"index":"contracts_id"})
            ids_bigquery_contract = df_bigquery_contracts["contracts_id"]
            max_index = ids_bigquery_contract.max()
            df_contratos['contracts_id'] = df_contratos.index + max_index
            df_to_insert_contracts = df_contratos[['contracts_id', 'Nombre del proyecto','Firma del contrato','sum_regalías','Vencimiento del contrato']]
            df_to_insert_contracts = df_to_insert_contracts.rename(columns={"Nombre del proyecto": "project_name", "Firma del contrato": "signature_date","sum_regalías":"general_royalties","Vencimiento del contrato":"expire_date"})
            df_to_insert_contracts = pd.concat([df_bigquery_contracts, df_to_insert_contracts])
            df_to_insert_contracts['expire_date'] = df_to_insert_contracts['expire_date'].fillna(np.nan).replace([np.nan], [None])
            df_to_insert_contracts['signature_date'] = df_to_insert_contracts['signature_date'].fillna(np.nan).replace([np.nan], [None])
            df_to_insert_contracts['expire_date'] = pd.to_datetime(df_to_insert_contracts['expire_date'], infer_datetime_format=True)
            df_to_insert_contracts['signature_date'] = pd.to_datetime(df_to_insert_contracts['signature_date'], infer_datetime_format=True)

            job_config = bigquery.LoadJobConfig(
                schema=[
                        bigquery.SchemaField("contracts_id", bigquery.enums.SqlTypeNames.INT64),
                        bigquery.SchemaField("project_name", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("signature_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                        bigquery.SchemaField("general_royalties", bigquery.enums.SqlTypeNames.FLOAT64),
                        bigquery.SchemaField("expire_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                ],
                write_disposition="WRITE_TRUNCATE",
            )

            call_bigquery_job(df_to_insert_contracts,'contracts_test', job_config)


        def set_contracts_creators(**kwargs):
            df_airtable_contratos = get_airtable_contratos()
            df_to_insert_contracts = get_bigquery('contracts_test')
            df_to_insert_creators = get_bigquery('creators_test')
            df_bigquery_contract_creators = get_bigquery('contracts_creators_test')
            df_contract_creators = df_airtable_contratos.merge(df_to_insert_contracts, left_on="Nombre del proyecto", right_on= "project_name",how="inner")
            df_contract_creators = df_contract_creators.merge(df_to_insert_creators,left_on="Creador",right_on="full_name",how="inner")
            df_contract_creators = df_bigquery_contract_creators.merge(df_contract_creators, on=['contracts_id','creator_id'],how="outer",indicator=True)
            df_contract_creators = df_contract_creators[df_contract_creators['_merge']=='right_only']
            df_contract_creators = df_contract_creators[['contracts_id','creator_id','Regalías','Anticipo']]
            df_contract_creators = df_contract_creators.reset_index()
            df_contract_creators = df_contract_creators.rename(columns={"index":"contracts_creators_id"})
            ids_bigquery_contract_creator = df_bigquery_contract_creators["contracts_creators_id"]
            max_index = ids_bigquery_contract_creator.max()
            df_contract_creators['contracts_creators_id'] = df_contract_creators.index + max_index
            df_to_insert_contract_creators = df_contract_creators[['contracts_creators_id','contracts_id','creator_id','Regalías','Anticipo']]
            df_to_insert_contract_creators = df_contract_creators.rename(columns={'Regalías':'individual_royalties','Anticipo':'money_advance'})
            df_to_insert_contract_creators = pd.concat([df_bigquery_contract_creators, df_to_insert_contract_creators])
            print(df_to_insert_contract_creators)

            job_config = bigquery.LoadJobConfig(
                schema=[
                        bigquery.SchemaField("contracts_creators_id", bigquery.enums.SqlTypeNames.INT64),
                        bigquery.SchemaField("contracts_id", bigquery.enums.SqlTypeNames.INT64),
                        bigquery.SchemaField("creator_id", bigquery.enums.SqlTypeNames.INT64),
                        bigquery.SchemaField("individual_royalties", bigquery.enums.SqlTypeNames.FLOAT64),
                        bigquery.SchemaField("money_advance", bigquery.enums.SqlTypeNames.FLOAT64),
                ],
                write_disposition="WRITE_TRUNCATE",
            )
            call_bigquery_job(df_to_insert_contract_creators,'contracts_creators_test', job_config)


        set_contents = PythonOperator(
            task_id="set_contents",
            python_callable=set_contents,
            on_failure_callback=notify_email,
            provide_context=True

        )
        set_creators = PythonOperator(
            task_id="set_creators",
            python_callable=set_creators,
            on_failure_callback=notify_email,
            provide_context=True

        )

        set_contracts = PythonOperator(
            task_id="set_contracts",
            python_callable=set_contracts,
            on_failure_callback=notify_email,
            provide_context=True

        )
        set_contracts_creators = PythonOperator(
            task_id="set_contracts_creators",
            python_callable=set_contracts_creators,
            on_failure_callback=notify_email,
            provide_context=True

        )


set_contents>>set_creators>>set_contracts>>set_contracts_creators
