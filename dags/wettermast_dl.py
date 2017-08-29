import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from moist_airflow.functions.pandas.df_update_db import df_update_another
from moist_airflow.functions.encode_wmascii_to_json import \
    encode_wmascii_to_json
from moist_airflow.functions.pandas.df_extract_columns import df_extract_columns
from moist_airflow.operators.check_file_available import FileAvailableOperator
from moist_airflow.operators.ftp_downloader import FTPDownloader
from moist_airflow.operators.ftp_sensor import FTPSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2015, 6, 1),
    'email': ['tfinn@live.com', ],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

FILE_PATH = '/home/tfinn/Data/test/measurement/wettermast'

dag = DAG('extract_wettermast', default_args=default_args,
          schedule_interval=datetime.timedelta(minutes=15),
          orientation='TB')

wm_sensor_task = FTPSensor(filename_template='%G_W%V_MASTER_M10.txt',
                           ftp_conn_id='ftp_wettermast',
                           disk_path=FILE_PATH,
                           task_id='sensor_ftp',
                           timeout=120,
                           poke_interval=10,
                           dag=dag)

dl_task = FTPDownloader(filename_template='%G_W%V_MASTER_M10.txt',
                        ftp_conn_id='ftp_wettermast',
                        disk_path=FILE_PATH,
                        task_id='downloader_ftp',
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        dag=dag)

already_dl_task = FileAvailableOperator(
    parent_dir=FILE_PATH,
    filename_template='%G_W%V_MASTER_M10.txt',
    task_id='file_checker',
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag)

encode_wm = PythonOperator(
    python_callable=encode_wmascii_to_json,
    op_kwargs=dict(
        input_path=FILE_PATH,
        output_path='/tmp'
    ),
    task_id='encoder_temp',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
    provide_context=True
)

todb_wm = PythonOperator(
    python_callable=df_update_another,
    op_kwargs=dict(
        input_path=FILE_PATH,
        input_template='wm.json',
        another_path='/tmp',
        another_template='wettermast_%Y%m%d%H%M.json',
        time_bound=datetime.timedelta(days=7)
    ),
    provide_context=True,
    task_id='add_to_db',
    dag=dag
)

prepare_plot = PythonOperator(
    python_callable=df_extract_columns,
    op_kwargs=dict(
        input_path=FILE_PATH,
        input_template='wm.json',
        output_path=FILE_PATH,
        output_template='plot_obs.json',
        column_names='TT002_M10'
    ),
    provide_context=True,
    task_id='extract_tt002',
    dag=dag
)

dl_task.set_upstream(wm_sensor_task)
already_dl_task.set_upstream(wm_sensor_task)
encode_wm.set_upstream(dl_task)
encode_wm.set_upstream(already_dl_task)
todb_wm.set_upstream(encode_wm)
prepare_plot.set_upstream(todb_wm)
