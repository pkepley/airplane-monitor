from pathlib import Path
import pandas as pd
import sqlite3
from datetime import datetime, timedelta


def pull_hourly(db_path: str | Path, start_dt_str: str | None = None):
    # TODO: this ignores Timezones. make it not do that.
    if start_dt_str is None:
        start_dt_time = datetime.combine(datetime.now(), datetime.min.time())
        start_dt_str = datetime.strftime(start_dt_time, "%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(db_path) as conn:
        q_hourly = """
        select
            strftime("%Y-%m-%d %H:00:00", datetime(time, 'unixepoch')) as hour,
            count(*) as n_obs,
            count(distinct hex_code) as n_hex,
            count(distinct flight) as n_flight
        from plane_observations
        where time > strftime('%s', datetime(?, 'localtime'))
        group by hour
        order by hour
        ;
        """
        return pd.read_sql_query(
            q_hourly,
            conn,
            params=(start_dt_str,),
            parse_dates={"hour": {"format": "%Y-%m-%d %H:%M:%S"}},
        )


def pull_hourly2(
    agg_db_path, start_dt: datetime | None = None, end_dt: datetime | None = None
) -> pd.DataFrame:
    # TODO: this ignores Timezones. make it not do that.
    if start_dt is None:
        start_dt = datetime.combine(datetime.now(), datetime.min.time())

    # TODO: this ignores Timezones. make it not do that.
    if end_dt is None:
        end_dt = datetime.now()

    # convert to string for sqlite
    start_dt_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_dt_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(agg_db_path) as conn:
        dftmp = pd.read_sql_query(
            """
            SELECT * FROM plane_observations_hourly
            WHERE hour > ?
            AND hour <= ?;
            """,
            conn,
            params=(start_dt_str, end_dt_str),
            parse_dates={"hour": {"format": "%Y-%m-%d %H:%M:%S"}},
        )
        return dftmp


class PlaneSummary:
    def __init__(self, db_path_raw: str | Path, db_path_agg: str | Path):
        self.db_path_raw = db_path_raw
        self.db_path_agg = db_path_agg
        self.init_agg_db()

    def init_agg_db(self):
        q_new = """CREATE TABLE IF NOT EXISTS plane_observations_hourly(
            hour TEXT PRIMARY KEY,
            n_obs INTEGER,
            n_hex INTEGER,
            n_flight INTEGER
        );"""

        with sqlite3.connect(self.db_path_agg) as conn:
            cur = conn.cursor()
            cur.execute(q_new)

    @property
    def first_hour_raw(self) -> datetime:
        q = """SELECT
        STRFTIME(\"%Y-%m-%d %H:00:00\", DATETIME(MIN(time), 'unixepoch'))
        FROM plane_observations;"""

        with sqlite3.connect(self.db_path_raw) as conn:
            cur = conn.cursor()
            cur.execute(q)
            res = cur.fetchone()
            res = res[0]

        res = datetime.strptime(res, "%Y-%m-%d %H:%M:%S")
        return res

    @property
    def last_hour_raw(self) -> datetime:
        q = """SELECT
        STRFTIME(\"%Y-%m-%d %H:00:00\", DATETIME(MAX(time), 'unixepoch'))
        FROM plane_observations;"""

        with sqlite3.connect(self.db_path_raw) as conn:
            cur = conn.cursor()
            cur.execute(q)
            res = cur.fetchone()
            res = res[0]

        res = datetime.strptime(res, "%Y-%m-%d %H:%M:%S")
        return res

    @property
    def first_hour_agg(self) -> datetime:
        q = """SELECT
        MIN(STRFTIME(\"%Y-%m-%d %H:00:00\", DATETIME(hour)))
        FROM plane_observations_hourly;"""

        with sqlite3.connect(self.db_path_agg) as conn:
            cur = conn.cursor()
            cur.execute(q)
            res = cur.fetchone()
            res = res[0]

        res = datetime.strptime(res, "%Y-%m-%d %H:%M:%S")
        return res

    @property
    def last_hour_agg(self) -> datetime:
        q = """SELECT
        MAX(STRFTIME(\"%Y-%m-%d %H:00:00\", DATETIME(hour)))
        FROM plane_observations_hourly;"""

        with sqlite3.connect(self.db_path_agg) as conn:
            cur = conn.cursor()
            cur.execute(q)
            res = cur.fetchone()
            res = res[0]

        res = datetime.strptime(res, "%Y-%m-%d %H:%M:%S")
        return res

    def pull_agg_raw(
        self, start_hour: datetime | None = None, end_hour: datetime | None = None
    ) -> pd.DataFrame:
        if start_hour is not None and end_hour is not None:
            filter = """
            WHERE time >= strftime('%s', ?) AND time < strftime('%s', ?)
            """
            params = (start_hour, end_hour)
        elif start_hour is None and end_hour is not None:
            filter = "WHERE time < strftime('%s', ?)"
            params = (end_hour,)
        elif start_hour is not None and end_hour is None:
            filter = "WHERE time >= strftime('%s', ?)"
            params = (start_hour,)
        else:
            filter = ""
            params = None

        q_hourly = f"""SELECT
            strftime("%Y-%m-%d %H:00:00", datetime(time, 'unixepoch')) as hour,
            COUNT(*) as n_obs,
            COUNT(distinct hex_code) as n_hex,
            COUNT(distinct flight) as n_flight
        FROM plane_observations
        {filter}
        GROUP BY hour
        ORDER BY hour
        ;"""

        with sqlite3.connect(self.db_path_raw) as conn:
            cur = conn.cursor()

            if params is not None:
                cur.execute(q_hourly, params)
            else:
                cur.execute(q_hourly)
            rslts = cur.fetchall()

        return rslts

    def update_agg_db(self):
        # get the last completely observed hour in the database. it can be None
        # if not None, then we want to only consider times *after* the start of
        # the *next* hour since the last completely observed hour
        prev_hour = self.last_hour_agg

        if prev_hour is not None:
            prev_hour = prev_hour + timedelta(hours=1)

        # what was the start of the current hour?
        curr_hour = datetime.strptime(
            datetime.utcnow().strftime("%Y-%m-%d %H:00:00"), "%Y-%m-%d %H:%M:%S"
        )

        # get new records
        print(f"Getting records between {prev_hour} and {curr_hour}")
        new_recs = self.pull_agg_raw(start_hour=prev_hour, end_hour=curr_hour)
        print(f"Found {len(new_recs)} new records.")

        if new_recs:
            q = """
            INSERT INTO plane_observations_hourly(
               hour, n_obs, n_hex, n_flight
            ) VALUES (?,?,?,?);
            """

            with sqlite3.connect(self.db_path_agg) as conn:
                cur = conn.cursor()
                cur.executemany(q, new_recs)
