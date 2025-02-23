from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'test-scheduler',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 28),
    'email': ['hemachandrarao.kuppuswamynaidu@directlinegroup.co.uk'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_premiumcontract_singlequery', default_args=default_args, schedule_interval=None)

t1 = BashOperator(
    task_id='premium_contract_test',
    bash_command='git clone https://github.com/Direct-Line-Group/fint-testing.git; '
                 'source /home/airflow/test-scheduler/env/bin/activate;'
                 'cd fint-testing; export postgresusername=fdp_test; '
                 'export env={{dag_run.conf["env"] if dag_run else ""}};'
                 'export batchkeys={{dag_run.conf["batchkeys"] if dag_run else ""}};'
                 'export database={{dag_run.conf["database"] if dag_run else ""}};'
                 'export postgrespassword=<password>;'
                 'python -m pytest tests/premium_fdp_integration/test_premium_singlequery.py;'
                 'aws s3 cp reports s3://fdp-test-automation-reports-test/premium/singlequery --recursive',
    retries=0,
    dag=dag)