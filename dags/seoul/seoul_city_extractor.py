# Import External Modules
import datetime as dt
import pandas as pd

# Import airflow related
import airflow
from airflow import DAG
from airflow.decorators import task

# Import plugins [~/airflow/plugins]
from seoul import SeoulCityExtractor
from seoul.seoul_city_extractor import SeoulPrivate
from seoul.seoul_city_extractor import AREA_CDS
from common import DBController

# Dag 
with DAG(
    dag_id="seoul_city_extractor",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=['seoul']
) as dag:
    @task
    def extract_seoul_city(extractor) -> dict:
        data_frame_dict = {'pptln': [], 'traffic_meta': [], 'traffic_detail': []}
        for area_cd in AREA_CDS:
            extractor.load_resp(area_cd)
            data_frame_dict['pptln'].append(extractor.get_pptln_df())
            data_frame_dict['traffic_meta'].append(extractor.get_road_traffic_meta_df())
            data_frame_dict['traffic_detail'].append(extractor.get_road_traffic_detail_df())
        return data_frame_dict
    
    def get_df_factory(data_nm):
        @task(task_id=f"get_{data_nm}_df")
        def get_df(data_frame_dict):
            df = pd.concat(data_frame_dict[data_nm],axis=0)
            return df
        return get_df
    
    def df2db_factory(data_nm):
        @task(task_id=f"{data_nm}_df2db")
        def df2db(df, db_controller):
            result = df.to_sql(name=data_nm,con=db_controller.engine,if_exists='append')
            return result
        return df2db
    
    seoul_city_extractor = SeoulCityExtractor(SeoulPrivate.api_key)
    db_controller = DBController(SeoulPrivate.db_cfg)
    
    # Extract
    data_frame_dict = extract_seoul_city(seoul_city_extractor)
    
    # Load pptln
    pptln_df_from_dict = get_df_factory('pptln')
    pptln_df = pptln_df_from_dict(data_frame_dict)
    pptln_df2db = df2db_factory('pptln')
    pptln_df2db(pptln_df, db_controller)
    
    # Load traffic_meta
    trafic_meta_df_from_dict = get_df_factory('traffic_meta')
    trafic_meta_df = trafic_meta_df_from_dict(data_frame_dict)
    trafic_meta_df2db = df2db_factory('traffic_meta')
    trafic_meta_df2db(trafic_meta_df, db_controller)
    
    # Load traffic_detail
    trafic_detail_df_from_dict = get_df_factory('traffic_detail')
    trafic_detail_df = trafic_detail_df_from_dict(data_frame_dict)
    trafic_detail_df2db = df2db_factory('traffic_detail')
    trafic_detail_df2db(trafic_detail_df, db_controller)