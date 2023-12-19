# Imports
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#from etl_scripts.extract import extract_data
from etl_scripts.transform_new2 import transform_data
from etl_scripts.load_new import load_data
from etl_scripts.load_new import load_fact_data



from datetime import datetime
import os

# ----------------------------------------------------- #
# ----------------------------------------------------- #

# ----------------------------------------------------- #
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/healthcare_{{ ds }}.csv'

neonatal_path = 'dags/data/neonatal.csv'
skilled_health_path = 'dags/data/skilled_health.csv'
antenatal_path = 'dags/data/antenatal.csv'
maternal_mortality_path = 'dags/data/maternal_mortality.csv'

PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'

with DAG(
    dag_id="healthcare_dag",
    start_date=datetime(2023,12,1),
    schedule_interval='@yearly'
)as dag:

# ......operators
    #transform data
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'neonatal_csv_path':neonatal_path,
            'skilled_health_csv_path': skilled_health_path,
            'antenatal_csv_path':antenatal_path,
            'maternal_mortality_csv_path':maternal_mortality_path,
            'target_dir': PQ_TARGET_DIR,
            'to_parquet':False,
            'to_csv':True
        }
    )
    # load year dim data
    load_year_dim = PythonOperator(
        task_id="load_year_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/year_dim_df.csv',
            'table_name': 'year_dim',
            'key': 'Year_Id'
        }
    )
    # load country type dim data
    load_country_dim = PythonOperator(
        task_id="load_country_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/country_dim_df.csv',
            'table_name': 'country_dim',
            'key': 'Country_Id'
        }
    )
    # load region dim data
    load_region_dim = PythonOperator(
        task_id="load_region_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/region_dim_df.csv',
            'table_name': 'region_dim',
            'key': 'WHO_Region_Id'
        }
    )
    # load income dim data
    load_income_dim = PythonOperator(
        task_id="load_income_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/income_dim_df.csv',
            'table_name': 'income_dim',
            'key': 'WB_Income_Group_Id'
        }
    )
    # load neoenatal dim data
    load_neonatal_dim = PythonOperator(
        task_id="load_neonatal_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/neonatal_dim_df.csv',
            'table_name': 'neonatal_dim',
            'key': 'NMR_Id'
        }
    )# load maternal mortality dim data
    load_maternal_mortality_dim = PythonOperator(
        task_id="load_maternal_mortality_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/maternal_mortality_dim_df.csv',
            'table_name': 'maternal_mortality_dim',
            'key': 'MMR_Id'
        }
    )# load antenatal dim data
    load_antenatal_dim = PythonOperator(
        task_id="load_antenatal_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/antenatal_dim_df.csv',
            'table_name': 'antenatal_dim',
            'key': 'ACC_Id'
        }
    )# load income dim data
    load_skilled_health_dim = PythonOperator(
        task_id="load_skilled_health_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/skilled_health_dim_df.csv',
            'table_name': 'skilled_health_dim',
            'key': 'SHP_Id'
        }
    )
    # load outcomes fact data
    load_healthcare_disparities_fct = PythonOperator(
        task_id="load_healthcare_disparities_fct",
        python_callable=load_fact_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR + '/healthcare_disparities_fct_df.csv',
            'table_name': 'healthcare_disparities_fct'
        }
    )


transform >> [load_year_dim, load_country_dim, load_region_dim, load_income_dim, load_neonatal_dim, load_maternal_mortality_dim, load_antenatal_dim, load_skilled_health_dim] >> load_healthcare_disparities_fct
