from airflow import DAG
from datetime import timedelta, datetime
from random import uniform, randint
from airflow.operators.python import PythonOperator, BranchPythonOperator

argumentos = {
    'owner': 'Italo Costa',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def _com_qualidade():
    valida = randint(0, 1)
    if valida:
        print(f'Encaminhando direto p/ a logística ({valida})')
        return 'logistica'
    else:
        print(f'Aguardando pedidos ({valida})')
        return 'estoque'
    

def _estoque():
    return 2

def _logistica():
    return 3


with DAG(
    default_args=argumentos,
    dag_id='postest_peças_OK',
    start_date=datetime(2022, 5, 31, 11, 0, 0),
    # schedule_interval='*/5 * * * *',
    schedule_interval=None
) as dag:

    com_qualidade = BranchPythonOperator(
        task_id='com_qualidade',
        python_callable=_com_qualidade
    )

    estoque = PythonOperator(
        task_id='estoque',
        python_callable=_estoque,
        trigger_rule = 'none_failed_or_skipped'
    )

    logistica = PythonOperator(
        task_id = 'logistica',
        python_callable=_logistica,
        trigger_rule = 'none_failed_or_skipped'
    )
    
    com_qualidade >> [estoque, logistica]
    


