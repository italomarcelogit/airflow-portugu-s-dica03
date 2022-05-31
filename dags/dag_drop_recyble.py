from airflow import DAG
from datetime import timedelta, datetime
from random import randint
from airflow.operators.python import PythonOperator, BranchPythonOperator

argumentos = {
    'owner': 'Italo Costa',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def _sem_qualidade():
    valida = randint(0, 1)
    if valida:
        print(f'Embalagem com problema, solicitar validação. ({valida})')
        return 'notifica_area'
    else:
        print(f'Peças com problemas. Descarte e processo de reciclagem ({valida})')
        return 'reciclagem'

def _notifica_area():
    return -1

def _reciclagem():
    return -2




with DAG(
    default_args=argumentos,
    dag_id='postest_to_recycle',
    start_date=datetime(2022, 5, 31, 11, 0, 0),
    # schedule_interval='*/5 * * * *',
    schedule_interval=None
) as dag:

    sem_qualidade = BranchPythonOperator(
        task_id = 'sem_qualidade',
        python_callable=_sem_qualidade
    )

    reciclagem = PythonOperator(
        task_id='reciclagem',
        python_callable=_reciclagem,
        trigger_rule = 'none_failed_or_skipped'
    )

    notifica_area = PythonOperator(
        task_id='notifica_area',
        python_callable=_notifica_area,
        trigger_rule = 'none_failed_or_skipped'
    )
    
    sem_qualidade >> [notifica_area, reciclagem]
   


