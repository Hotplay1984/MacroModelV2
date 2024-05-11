import datetime as dt
import pandas as pd
import numpy as np
from IPython.display import display
from WD.dm_wd_data import wdAPI
from MacroModelV2 import config 
from TS.dm_utils import check_time_series, transform_to_monthly, to_yoy
import TS.dm_ts_config as ts_config

class Data:
    def __init__(self):
        self.wd = wdAPI()
        self.load_config()
        self.df_raw = None
        self.df_check = None
        self.df_monthly = None

    def load_config(self):
        """Load configuration from Excel file."""
        self.df_config = pd.read_excel(config.config_file, index_col='指标ID')
        self.bng_date = self.df_config['开始日期'].min().strftime('%Y-%m-%d')
        self.end_date = dt.datetime.now().strftime('%Y-%m-%d')

    def load_data(self):
        self.read_raw_data()
        self.get_monthly_data()

    def read_raw_data(self, wd_id):
        """Fetch and process raw data from wdAPI."""
        if isinstance(wd_id, list):
            wd_id = ','.join(wd_id)
        df_raw = self.wd.fetch_data(self.wd.w.edb, [wd_id, self.bng_date, self.end_date])
        df_raw = df_raw.apply(self.apply_transformations)
        self.run_raw_data_check(df_raw)
        self.df_raw = df_raw

    def apply_transformations(self, series):
        """Apply transformations based on configuration."""
        s_type = ts_config.s_type_mapping[self.df_config.loc[series.name, '变换类型']]
        if s_type in ['xox', 'yoy', 'ratio']:
            return series * 0.01
        return series

    def run_raw_data_check(self, df_raw):
        """Check time series data and display results."""
        df_check = check_time_series(df_raw)
        df_check['Name'] = [self.df_config.loc[wd_code, '指标名称'] for wd_code in df_check.index]
        df_check = df_check[['Name'] + df_check.columns.tolist()]
        self.df_check = df_check
        display(df_check)

    def get_monthly_data(self):
        df_raw = self.df_raw
        df_config = self.df_config
        df_monthly = pd.DataFrame()
        for variable in df_raw.columns.tolist():
            freq = ts_config.freq_mapping[df_config.loc[variable, '频率']]
            s_type = ts_config.s_type_mapping[df_config.loc[variable, '变换类型']]
            s_raw = df_raw[variable].copy()
            s_monthly = transform_to_monthly(s_raw, freq=freq, s_type=s_type)
            if s_type != 'yoy':
                method = 'div' if s_type != 'ratio' else 'diff'
                s_monthly = to_yoy(s_monthly, freq=s_monthly.index.freq, method=method)
            df_monthly = df_monthly.merge(s_monthly, how='outer', left_index=True, right_index=True)
        self.df_monthly = df_monthly
        return
