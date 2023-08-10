import json
from datetime import datetime, timedelta
from pytz import timezone

from dash import Dash, html, dcc, callback, Output, Input
import plotly.graph_objects as go

import pandas as pd

from airplane_monitor import ConfigReader
from airplane_monitor import PlaneSummary
from airplane_monitor import Analysis

# allow us to read config
cr = ConfigReader()
app_timezone = cr.timezone
app_base_url = cr.base_url


def _midnight(src_naive_datetime: datetime) -> datetime:
    midnight = src_naive_datetime.astimezone(timezone(app_timezone))
    midnight = datetime.combine(midnight, midnight.min.time())
    midnight = midnight.astimezone(timezone(app_timezone))
    return midnight


def midnight_today() -> datetime:
    src_naive_datetime = datetime.now()
    midnight = _midnight(src_naive_datetime)
    return midnight


def week_start() -> datetime:
    today = datetime.now()
    src_naive_datetime = today - timedelta(days=today.weekday())
    midnight = _midnight(src_naive_datetime)
    return midnight


def decompose_frame(aa: Analysis) -> pd.DataFrame:
    res = aa.decompose_series()
    trend_df = pd.DataFrame(res.trend)
    season_df = res.seasonal
    df_res = pd.merge(trend_df, season_df, left_index=True, right_index=True)

    return df_res


@callback(
    Output("raw-frame", "data"),
    Output("decompose-frame", "data"),
    Input("graph-last-date", "date"),
    Input("weeks-allowed", "value"),
)
def update_dataframes(last_date_str: str, n_weeks: int):
    # last_date comes from the app. it reflects a date in *LOCAL* time
    # so we need to make that date timezone aware and then convert to UTC
    date_end = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
    date_end = timezone(app_timezone).localize(date_end)
    date_end = date_end.astimezone(timezone("utc"))

    date_start = date_end + timedelta(days=-n_weeks * 10)

    # allow us to read config
    cr = ConfigReader()

    # update analysis db
    ps = PlaneSummary(cr.db_path_raw, cr.db_path_agg)
    ps.update_agg_db()

    # set the analysis
    aa = Analysis(cr.db_path_raw, cr.db_path_agg)

    # get the raw frame
    df = aa.pull_hourly(date_start, date_end)

    # perform the decomposition
    df_decomp = decompose_frame(aa)

    return df.to_json(date_format="iso"), df_decomp.to_json(date_format="iso")


@callback(
    Output("graph-time-series", "figure"),
    Input("raw-frame", "data"),
    Input("decompose-frame", "data"),
)
def get_graph_time_series(df_raw_str: str, df_decomp_str: str) -> go.Figure:
    df = pd.DataFrame(json.loads(df_raw_str))
    df.index = pd.DatetimeIndex(df.index).tz_convert(app_timezone)

    res = pd.DataFrame(json.loads(df_decomp_str))
    res.index = pd.DatetimeIndex(res.index).tz_convert(app_timezone)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df.index, y=df.n_flight, mode="lines", name="Planes<br>Observed")
    )
    fig.add_trace(
        go.Scatter(x=res.trend.index, y=res.trend, mode="lines", name="Trend")
    )
    fig.update_layout(
        title="Planes Obeserved Per Hour vs Time",
        xaxis_title="Time",
        yaxis_title="Planes Observed Per Hour",
        legend_title="Legend",
        font=dict(
            family="Courier New, monospace",
            # size=18
        ),
    )
    return fig


@callback(Output("graph-hourly", "figure"), Input("decompose-frame", "data"))
def get_graph_hourly(df_decomp_str: str) -> go.Figure:
    'Returns a scatter-plot for range + today\'s line for hourly "seasonality"'

    df_s = pd.DataFrame(json.loads(df_decomp_str))
    df_s.index = pd.DatetimeIndex(pd.to_datetime(df_s.index)).tz_convert(app_timezone)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df_s.index.hour, y=df_s.seasonal_24, mode="markers", name="Range"),
    )
    start_time = midnight_today()
    df_s_today = df_s[df_s.index >= start_time]
    fig.add_trace(
        go.Scatter(
            x=df_s_today.index.hour,
            y=df_s_today.seasonal_24,
            mode="lines+markers",
            name="Today",
        )
    )
    fig.update_layout(
        title="Hourly Trend Offsets",
        xaxis_title="Hour of Day",
        yaxis_title="Trend Offset",
        legend_title="Legend",
        font=dict(
            family="Courier New, monospace",
            # size=18
        ),
    )
    return fig


# scatter-plot for range + this week's line for weekly "seasonality"
@callback(Output("graph-weekly", "figure"), Input("decompose-frame", "data"))
def get_graph_weekly(df_decomp_str: str) -> go.Figure:
    'Returns a scatter-plot for range + this week\'s line for weekly "seasonality"'

    # weekly data
    df_w = pd.DataFrame(json.loads(df_decomp_str))
    df_w.index = pd.DatetimeIndex(pd.to_datetime(df_w.index)).tz_convert(app_timezone)

    weekly = df_w[["seasonal_168"]].resample("d").mean()
    weekly["week_day_num"] = weekly.index.day_of_week
    weekly["week_day"] = weekly.index.day_name()
    weekly_mean = (
        weekly.groupby(["week_day_num", "week_day"], as_index=False)
        .mean()
        .sort_values("week_day_num")
    )
    df_w = weekly

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_w.index.weekday, y=df_w.seasonal_168, mode="markers", name="Range"
        )
    )
    start_time = week_start()
    df_w_w = df_w.loc[df_w.index >= start_time]
    fig.add_trace(
        go.Scatter(
            x=df_w_w.index.weekday,
            y=df_w_w.seasonal_168,
            mode="lines+markers",
            name="Current<br>Week",
        )
    )
    fig.update_layout(
        title="Daily Trend Offsets",
        xaxis=dict(
            tickmode="array",
            tickvals=weekly_mean.week_day_num,
            ticktext=weekly_mean.week_day,
        ),
        xaxis_title="Weekday",
        yaxis_title="Trend Offset",
        legend_title="Legend",
        font=dict(
            family="Courier New, monospace",
            # size=18
        ),
    )
    return fig


def serve_layout():
    "Function to serve the layout"

    slayout = html.Div(
        [
            html.H1(children="Recent Plane Summary", style={"textAlign": "center"}),
            dcc.DatePickerSingle(id="graph-last-date", date=datetime.today().date()),
            dcc.Input(
                id="weeks-allowed",
                type="number",
                value=10,
                min=2,
                max=52,
                step=1,
                debounce=True,
            ),
            dcc.Graph(id="graph-time-series"),
            dcc.Graph(id="graph-hourly"),
            dcc.Graph(id="graph-weekly"),
            dcc.Store(id="raw-frame"),
            dcc.Store(id="decompose-frame"),
        ]
    )

    return slayout


# the app itself
app = Dash(__name__, url_base_pathname=f"/{app_base_url}/")
app.layout = serve_layout


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8053)
