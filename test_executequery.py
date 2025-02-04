import psycopg2
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd

from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'test-scheduler',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 10),
    'email': ['hemachandrarao.kuppuswamynaidu@directlinegroup.co.uk'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


def get_connection(connection_name):
    test_aurora_postgres_connection = psycopg2.connect(user="fdp_test",
                                                       password="",
                                                       host="ifrs-test-aurora-cluster.cluster-cnolo9owvhht.eu-west-1.rds.amazonaws.com",
                                                       port="5432",
                                                       database="postgres")
    test_aurora_dapload_connection = psycopg2.connect(user="fdp_test",
                                                      password="",
                                                      host="ifrs-test-aurora-cluster.cluster-cnolo9owvhht.eu-west-1.rds.amazonaws.com",
                                                      port="5432",
                                                      database="dapload")
    test_redshift_ifrstestredshift_connection = psycopg2.connect(user="ifrstestredshiftown",
                                                                 password="",
                                                                 host="ifrstestredshiftclusternew.c7j7caayqsxn.eu-west-1.redshift.amazonaws.com",
                                                                 port="5439",
                                                                 database="ifrstestredshift")
    test_redshift_dapload_connection = psycopg2.connect(user="ifrstestredshiftown",
                                                        password="",
                                                        host="ifrstestredshiftclusternew.c7j7caayqsxn.eu-west-1.redshift.amazonaws.com",
                                                        port="5439",
                                                        database="dapload")
    if connection_name == 'aurora_postgres':
        return test_aurora_postgres_connection
    elif connection_name == 'aurora_dapload':
        return test_aurora_dapload_connection
    elif connection_name == 'redshift_ifrstestredshift':
        return test_redshift_ifrstestredshift_connection
    elif connection_name == 'redshift_dapload':
        return test_redshift_dapload_connection
    else:
        return 'invalid connection name'


def run_this_func(ds, **kwargs):
    query = kwargs['dag_run'].conf['query'] + " LIMIT 1000"
    df = pd.read_sql_query(query, get_connection(kwargs['dag_run'].conf['connection']))
    print("===================================================================================")
    print(query)
    print("===================================================================================")
    print(df.values)
    print("===================================================================================")


dag = DAG(
    'test_executequery', default_args=default_args, schedule_interval=None)

run_this = PythonOperator(task_id='test_executequery', provide_context=True, python_callable=run_this_func, dag=dag, )

