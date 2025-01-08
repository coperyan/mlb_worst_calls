import os
import re
import io
import warnings
import requests
import pandas as pd
import concurrent.futures
from tqdm import tqdm

from _helpers import get_date, get_date_range
from analysis import *

warnings.simplefilter("ignore")

_BASE_URL = "https://baseballsavant.mlb.com"
_DATE_FORMATS = [
    (re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$"), "%Y-%m-%d"),
    (
        re.compile(r"^\d{4}-\d{1,2}-\d{1,2}T\d{2}:\d{2}:\d{2}.\d{1,6}Z$"),
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ),
]
_VALID_KWARGS = [
    "start_date",
    "end_date",
    "game_pks",
    "batter_ids",
    "pitcher_ids",
    "teams",
    "pitch_types",
    "events",
    "descriptions",
]
_SORT_KEYS = ["game_pk", "at_bat_number", "pitch_number"]
_FILL_NA_COLS = ["plate_x", "plate_z", "sz_bot", "sz_top"]


class Statcast:

    def __init__(self):
        self.kwargs = None

        self.iteration_type = None
        self.iterations = []
        self.urls = []
        self.data = []

        self.df = None

    def _validate_kwargs(self) -> bool:
        bad_kwargs = {k for k in self.kwargs.keys() if k not in _VALID_KWARGS}
        if bad_kwargs:
            raise ValueError(f"Bad kwargs passed to function: {bad_kwargs}")
        else:
            return True

    def _cleanup_kwargs(self):
        for k, v in self.kwargs.items():
            if k not in ["start_date", "end_date"] and not isinstance(v, list):
                self.kwargs[k] = [v]

        if "start_date" in self.kwargs and "end_date" not in self.kwargs:
            self.kwargs["end_date"] = get_date(days_ago=1)

    def _identify_iteration_type(self):
        if self.kwargs.get("game_pks"):
            print(f"Validated args, iterating by games.")
            self.iteration_type = "games"
            self.iterations = self.kwargs.get("game_pks")
        elif self.kwargs.get("start_date"):
            print(f"Validated args, iterating by dates.")
            date_range = get_date_range(
                self.kwargs.get("start_date"), self.kwargs.get("end_date")
            )
            self.iteration_type = "dates"
            self.iterations = date_range
        else:
            raise RuntimeError(
                f"Must pass either start_date or games to API for iterative use."
            )

    def _generate_urls(self):
        for itrn in self.iterations:
            self.urls.append(self._build_url(itrn))

    def _build_url(self, iter_val) -> str:
        """Build Statcast API Request URL

        For each of the main parameters in this class's init --
            Format, add to the URL's params

        Parameters
        ----------
                Value being iterated over for the current iteration
                i.e. 2023-09-01, 2023-09-02, etc.

        Returns
        -------
            str
                Request URL for self._make_request()
        """
        base_url = _BASE_URL + "/statcast_search/csv?all=true&type=details"

        if self.kwargs.get("pitch_types"):
            base_url += "&hfPT=" + "".join(
                [f"{x.upper()}|" for x in self.kwargs.get("pitch_types")]
            )

        if self.kwargs.get("events"):
            base_url += "&hfAB=" + "".join(
                [f"{x}|".replace(" ", "\\.\\.") for x in self.kwargs.get("events")]
            )

        if self.kwargs.get("descriptions"):
            base_url += "&hfPR=" + "".join(
                [
                    f"{x}|".replace(" ", "\\.\\.")
                    for x in self.kwargs.get("descriptions")
                ]
            )

        if self.iteration_type == "games":
            base_url = base_url + "&game_pk=" + str(iter_val)

        elif self.iteration_type == "dates":
            base_url = (
                base_url + "&game_date_gt=" + iter_val + "&game_date_lt=" + iter_val
            )

        if self.kwargs.get("pitcher_ids"):
            base_url += "".join(
                [f"&pitchers_lookup[]={x}" for x in self.kwargs.get("pitcher_ids")]
            )

        if self.kwargs.get("batter_ids"):
            base_url += "".join(
                [f"&batters_lookup[]={x}" for x in self.kwargs.get("batter_ids")]
            )

        ##Handle teams
        if (
            self.iteration_type == "games"
            or self.kwargs.get("pitcher_ids")
            or self.kwargs.get("batter_ids")
        ) and self.kwargs.get("teams"):
            print(
                f"Team parameter passed, but game, pitcher or batter already specified.. Not applying team filter."
            )
        elif self.kwargs.get("teams"):
            base_url += "&player_type=pitcher|batter|&hfTeam=" + "".join(
                [f"{x}|" for x in self.kwargs.get("teams")]
            )

        return base_url

    def _make_request(self, url: str, **kwargs):
        resp = requests.get(url, timeout=None, **kwargs)
        return resp.content.decode("utf-8")

    def _concurrent_requests(self, **kwargs):
        with tqdm(total=len(self.urls)) as progress:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self._make_request, url, **kwargs)
                    for url in self.urls
                }
                for future in concurrent.futures.as_completed(futures):
                    self.data.append(future.result())
                    progress.update(1)

    def _parse_result_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse Statcast Dataframe

        Parameters
        ----------
            df : pd.DataFrame
                Dataframe -- result of statcast API request

        Returns
        -------
            pd.DataFrame
                Cleaned, parsed, normalized dataframe
        """
        str_cols = [
            dt[0] for dt in df.dtypes.items() if str(dt[1]) in ["object", "string"]
        ]

        for strcol in str_cols:
            fvi = df[strcol].first_valid_index()
            if fvi is None:
                continue
            fv = df[strcol].loc[fvi]

            if str(fv).endswith("%") or strcol.endswith("%"):
                df[strcol] = (
                    df[strcol].astype(str).str.replace("%", "").astype(float) / 100.0
                )
            else:
                for date_regex, date_format in _DATE_FORMATS:
                    if isinstance(fv, str) and date_regex.match(fv):
                        df[strcol] = df[strcol].apply(
                            pd.to_datetime, errors="ignore", format=date_format
                        )
                        df[strcol] = df[strcol].convert_dtypes(convert_string=False)
                        break

        df.rename(
            columns={
                col: col.replace(".", "_") for col in df.columns.values if "." in col
            },
            inplace=True,
        )
        return df

    def _resp_to_df(self) -> pd.DataFrame:
        df_list = []
        for d in self.data:
            df = pd.read_csv(io.StringIO(d))
            df = self._parse_result_df(df)
            if df is not None and not df.empty:
                if "error" in df.columns:
                    raise Exception(df["error"].values[0])
                else:
                    df_list.append(df)

        df = pd.concat(df_list, axis=0, ignore_index=True).convert_dtypes(
            convert_string=False
        )
        df = df.sort_values(_SORT_KEYS, ascending=True)
        df["pitch_id"] = df.apply(
            lambda x: "|".join([str(x.get(col)) for col in _SORT_KEYS]),
            axis=1,
        )
        df = df[["pitch_id"] + [col for col in df.columns.values if col != "pitch_id"]]
        df[_FILL_NA_COLS] = df[_FILL_NA_COLS].fillna(0)

        return df

    def search(self, **kwargs):
        self.kwargs = kwargs
        self._validate_kwargs()
        self._cleanup_kwargs()
        self._identify_iteration_type()
        self._generate_urls()
        self._concurrent_requests()
        if len(self.data) > 0:
            self.df = self._resp_to_df()
            print(f"Created dataframe, {len(self.df)} row(s)..")
        else:
            print("No data found..")

    def get_df(self) -> pd.DataFrame:
        return self.df

    def umpire_calls(self):
        self.df = umpire_calls(self.df)

    def delta_win_exp(self):
        self.df = delta_win_exp(self.df)

    def pitch_movement(self):
        self.df = pitch_movement(self.df)
