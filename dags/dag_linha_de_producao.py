from airflow import DAG
from datetime import timedelta, datetime
from random import uniform
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


argumentos = {
    'owner': 'Italo Costa',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def _producao():
    return 1

def _testedequalidade():
    teste = uniform(3.0, 9.99)
    if teste > 5:
        return 'com_qualidade'
    else:
        return 'sem_qualidade'






with DAG(
    default_args=argumentos,
    dag_id='test_lotes_peças_LP',
    start_date=datetime(2022, 5, 31, 11, 0, 0),
    schedule_interval='*/20 * * * *',
    # schedule_interval='@daily'
) as dag:

    producao = PythonOperator(
        task_id = 'producao',
        python_callable=_producao
    )

    teste = BranchPythonOperator(
        task_id='teste_de_qualidade',
        python_callable=_testedequalidade,
        do_xcom_push=False
    )

    com_qualidade = TriggerDagRunOperator(
        task_id='com_qualidade',
        trigger_dag_id='postest_peças_OK',
        execution_date='{{ ds }}', 
        reset_dag_run=True,
        wait_for_completion=True,
    )

    sem_qualidade = TriggerDagRunOperator(
        task_id = 'sem_qualidade',
        trigger_dag_id='postest_to_recycle',
        execution_date='{{ ds }}', 
        reset_dag_run=True,
        wait_for_completion=True,
    )

    producao >> teste 
    teste >> [com_qualidade, sem_qualidade]  
   


