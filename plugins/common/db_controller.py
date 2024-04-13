import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text


class DBController:
    def __init__(self, db_cfg) -> None:
        self.db_cfg = db_cfg
        self.engine = self.__create_engine()

    def __create_engine(self):
        db_cfg = self.db_cfg
        db_url = f"{db_cfg['base']}/{db_cfg['user_id']}:{db_cfg['user_pw']}@{db_cfg['url']}:{db_cfg['port']}/{db_cfg['db_name']}"
        engine = create_engine(url=db_url)
        return engine

    def read_sql(self, sql) -> pd.DataFrame:
        return pd.read_sql(sql=sql, con=self.engine)

    def execute_sql(self, sql) -> list:
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
        return result.fetchall()


