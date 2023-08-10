from datetime import datetime, timezone
import pandas as pd
from statsmodels.tsa.seasonal import MSTL
from airplane_monitor.PlaneSummary import PlaneSummary, pull_hourly2, thirty_day_start


class Analysis:
    def __init__(
        self, db_path_raw: str, db_path_agg: str, timezone: str = "US/Eastern"
    ):
        self.timezone = timezone
        self.db_path_raw = db_path_raw
        self.db_path_agg = db_path_agg
        self.plane_summary = PlaneSummary(self.db_path_raw, self.db_path_agg)

    def pull_hourly(
        self, start_dt_str: str | None, end_dt_str: str | None
    ) -> pd.DataFrame:
        if start_dt_str is None:
            start_dt_str = self.plane_summary.first_hour_agg

        if end_dt_str is None:
            end_dt_str = self.plane_summary.last_hour_raw

        df = pull_hourly2(self.db_path_agg, start_dt_str, end_dt_str)

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

    def blah(self):
        # plot weekly 'seasonality'
        weekly = self.res.seasonal[["seasonal_168"]].resample("d").mean()
        weekly["week_day_num"] = weekly.index.day_of_week
        weekly["week_day"] = weekly.index.day_name()

        # get mean, useful for extracting order
        weekly_mean = (
            weekly.groupby(["week_day_num", "week_day"], as_index=False)
            .mean()
            .sort_values("week_day_num")
        )


if __name__ == "__main__":
    from config_reader import ConfigReader

    aa = Analysis()
