from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from moist_airflow.wm_ftp_sensor import WettermastFTPSensor
from moist_airflow.wm_ftp_dl import WMFTPDownloader
from moist_airflow.wm_file_available import WMFileAvailable
from moist_airflow.wm_encode_to_temp import WMEncodeTemp
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['tfinn@live.com', ],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

FILE_PATH = '/home/tfinn/Data/test/measurement/wettermast'

dag = DAG('wm_test', default_args=default_args, schedule_interval=timedelta(7),
          orientation='TB')

wm_sensor_task = WettermastFTPSensor(disk_path=FILE_PATH,
                                     ftp_conn_id='ftp_wettermast',
                                     task_id='sensor_ftp_wettermast',
                                     dag=dag)

dl_task = WMFTPDownloader(disk_path=FILE_PATH,
                          ftp_conn_id='ftp_wettermast',
                          task_id='downloader_ftp_wettermast',
                          trigger_rule=TriggerRule.ALL_SUCCESS,
                          dag=dag)

already_dl_task = WMFileAvailable(disk_path=FILE_PATH,
                                  task_id='file_checker_wettermast',
                                  trigger_rule=TriggerRule.ALL_FAILED,
                                  dag=dag)

encode_wm = WMEncodeTemp(disk_path=FILE_PATH,
                         temp_path='/tmp',
                         task_id='encoder_temp_wettermast',
                         trigger_rule=TriggerRule.ONE_SUCCESS,
                         dag=dag)

dl_task.set_upstream(wm_sensor_task)
already_dl_task.set_upstream(wm_sensor_task)
encode_wm.set_upstream(dl_task)
encode_wm.set_upstream(already_dl_task)
