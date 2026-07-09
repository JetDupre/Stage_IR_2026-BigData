# dags/nutriscan_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'tom',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nutriscan_pipeline',
    default_args=default_args,
    schedule='0 12 * * *',  # tous les jours a 12h du matin (Airflow 2.4+)
    start_date=datetime(2026, 6, 1),
    catchup=False,
) as dag:

    # Staging
    t_stg_products  = BashOperator(task_id='stg_products',
        bash_command='python /opt/airflow/transform/staging/stg_products.py')
    t_stg_nutrition = BashOperator(task_id='stg_nutrition',
        bash_command='python /opt/airflow/transform/staging/stg_nutrition.py')
    t_stg_countries = BashOperator(task_id='stg_countries',
        bash_command='python /opt/airflow/transform/staging/stg_countries.py')
    t_stg_brands    = BashOperator(task_id='stg_brands',
        bash_command='python /opt/airflow/transform/staging/stg_brands.py')
    t_stg_date      = BashOperator(task_id='stg_date',
        bash_command='python /opt/airflow/transform/staging/stg_date.py')
    t_stg_category  = BashOperator(task_id='stg_category',
        bash_command='python /opt/airflow/transform/staging/stg_category.py')

    # Marts
    t_dim_product   = BashOperator(task_id='dim_product',
        bash_command='python /opt/airflow/transform/marts/dim_product.py')
    # A COMPLETER : t_dim_category, t_dim_brand, t_dim_country, t_dim_date
    t_dim_category  = BashOperator(task_id='dim_category',
        bash_command='python /opt/airflow/transform/marts/dim_category.py')
    t_dim_brand     = BashOperator(task_id='dim_brand',
        bash_command='python /opt/airflow/transform/marts/dim_brand.py')
    t_dim_country   = BashOperator(task_id='dim_country',
        bash_command='python /opt/airflow/transform/marts/dim_country.py')
    t_dim_date      = BashOperator(task_id='dim_date',
        bash_command='python /opt/airflow/transform/marts/dim_date.py')
    
    # A COMPLETER : t_fact_nutrition
    t_fact_nutrition = BashOperator(task_id='fact_nutrition',
        bash_command='python /opt/airflow/transform/marts/fact_nutrition.py')

    # Tests
    t_tests = BashOperator(task_id='tests_qualite',
        bash_command='python /opt/airflow/transform/tests/test_dim_product.py')

    # Dependances
    [t_stg_products, t_stg_nutrition, t_stg_countries, t_stg_brands, t_stg_date, t_stg_category] >> t_dim_product
    t_stg_category >> t_dim_category
    t_stg_brands >> t_dim_brand
    t_stg_countries >> t_dim_country
    t_stg_date >> t_dim_date
    [t_dim_product, t_dim_category, t_dim_brand, t_dim_country, t_dim_date] >> t_fact_nutrition

    # les tests doivent s'executer EN DERNIER
    t_fact_nutrition >> t_tests
