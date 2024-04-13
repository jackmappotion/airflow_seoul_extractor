import requests
import pandas as pd


class SeoulCityExtractor:
    """서울 실시간 도시데이터"""

    def __init__(self, api_key) -> None:
        self.api_key = api_key

    def get_pptln_df(self) -> pd.DataFrame:
        """실시간 인구데이터"""
        city_data_dict = self.city_data_dict.copy()
        ppltn_dict = city_data_dict["LIVE_PPLTN_STTS"][0]
        ppltn_dict["AREA_NM"] = city_data_dict["AREA_NM"]
        ppltn_dict["AREA_CD"] = city_data_dict["AREA_CD"]
        return pd.json_normalize(ppltn_dict)

    def get_road_traffic_meta_df(self) -> pd.DataFrame:
        """실시간 도로 메타 데이터"""
        city_data_dict = self.city_data_dict.copy()
        road_traffic_dict = city_data_dict["ROAD_TRAFFIC_STTS"]
        road_traffic_meta_dict = road_traffic_dict["AVG_ROAD_DATA"]
        road_traffic_meta_dict["AREA_NM"] = city_data_dict["AREA_NM"]
        road_traffic_meta_dict["AREA_CD"] = city_data_dict["AREA_CD"]
        return pd.json_normalize(road_traffic_meta_dict)

    def get_road_traffic_detail_df(self) -> pd.DataFrame:
        """실시간 도로 디테일 데이터"""
        city_data_dict = self.city_data_dict.copy()
        road_traffic_dict = city_data_dict["ROAD_TRAFFIC_STTS"]
        road_traffic_detail_dict = road_traffic_dict["ROAD_TRAFFIC_STTS"]
        road_traffic_detail_df = pd.json_normalize(road_traffic_detail_dict)
        road_traffic_detail_df["AREA_NM"] = city_data_dict["AREA_NM"]
        road_traffic_detail_df["AREA_CD"] = city_data_dict["AREA_CD"]
        return road_traffic_detail_df

    def __format_url(self, area_cd):
        base_url = "http://openapi.seoul.go.kr:8088"
        output_format = "json"
        service = "citydata"
        start_index = 1
        end_index = 1
        url = f"{base_url}/{self.api_key}/{output_format}/{service}/{start_index}/{end_index}/{area_cd}"
        return url

    def load_resp(self, area_cd):
        url = self.__format_url(area_cd)
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            self.city_data_dict = resp.json()["CITYDATA"]
        except requests.exceptions.HTTPError as e:
            self.city_data_dict = None
        return None

