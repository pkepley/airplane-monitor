from datetime import datetime
import pandas as pd
from statsmodels.tsa.seasonal import MSTL
from airplane_monitor.PlaneSummary import PlaneSummary, pull_hourly2


class Analysis:
    def __init__(self, db_path_raw: str, db_path_agg: str, tz_str: str = "US/Eastern"):
        self.timezone = tz_str
        self.db_path_raw = db_path_raw
        self.db_path_agg = db_path_agg
        self.plane_summary = PlaneSummary(self.db_path_raw, self.db_path_agg)

    def pull_hourly(
        self, start_dt: datetime | None, end_dt: datetime | None
    ) -> pd.DataFrame:
        if start_dt is None:
            start_dt = self.plane_summary.first_hour_agg

        if end_dt is None:
            end_dt = self.plane_summary.last_hour_raw

        df = pull_hourly2(self.db_path_agg, start_dt, end_dt)

        print(df)

        # set index and ensure all hourly observations are present
        # (we're currently missing any hours with 0 observations,
        #  which will screw up the timeseries modeling... so add them in!)
        df.set_index("hour", inplace=True)
        df = df.resample("H").asfreq().fillna(0)

        # convert from utc --> timezone
        df.index = df.index.tz_localize(tz="utc")
        df.index = df.index.tz_convert(tz=self.timezone)
        self.df = df

        return df

    def decompose_series(self, analysis_var="n_flight"):
        # fit time series model
        mstl = MSTL(self.df[analysis_var], periods=[24, 24 * 7])
        self.res = mstl.fit()

        return self.res
