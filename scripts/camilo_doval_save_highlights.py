import os
import time
from datetime import datetime, date

import logging
import logging.config

from mlb_videos.utils import setup_project2, x_days_ago, x_months_ago
from mlb_videos import MLBVideoClient

from mlb_videos.statsapi import Schedule, Game

## from mlb_videos.statcast import Statcast

_PROJECT_NAME = "camilo_doval_save_highlights"
_PLAYER_ID = 666808
_MAX_CLIP_LEN = 15

date_var = "2023"
start_date = "2023-03-30"
end_date = "2023-10-13"
project_path = setup_project2(_PROJECT_NAME, date_var)

logging.config.fileConfig(
    "logging.ini",
    defaults={"project_name": _PROJECT_NAME, "project_date": date_var},
)
logger = logging.getLogger(__name__)

# Get SF game schedule for date range, find Doval saves
schedule = Schedule(start_date=start_date, end_date=end_date, team="SF").get_df()
logging.info(f"Got schedule of {len(schedule)} games..")
games = Game(game_pks=[r.get("game_pk") for _, r in schedule.iterrows()]).get_df()
logging.info(f"Got detail for {len(games)} games..")
games = [
    r.get("game_pk") for _, r in games.iterrows() if r.get("save_id") == _PLAYER_ID
]

# games = [716381, 717674, 717375, 717702]

logging.info(f"Found {len(games)} save(s)..")

data = MLBVideoClient(
    project_name=_PROJECT_NAME,
    project_path=project_path,
    statcast_params={
        "games": games,
        "pitchers": [_PLAYER_ID],
    },
)
data.rank_df(
    name="pitch_in_game_rank",
    group_by=["game_pk"],
    fields=["pitch_id"],
    ascending=[False],
    keep_sort=False,
)
data.query_df("pitch_in_game_rank == 1 or events == 'strikeout'")
data._reset_df_sort()
data._get_filmroom_videos(params={"download": True, "feed": "best"})
data.compilation_params = {
    "use_intro": True,
    "add_transitions": True,
    "transition_padding": 0.5,
    "max_clip_length": 20,
}
data.create_compilation()
data.youtube_params = {
    "title": "Camilo Doval | Each of his 39 Saves (SF Giants 2023)",
    "description": "",
    "tags": [
        "mlb 2023",
        "best closer",
        "2023 giants",
        "san francisco giants",
        "camilo doval",
    ],
    "playlist": "San Francisco Giants - Player Highlights",
    "privacy": "private",
    "thumbnail": "resources/camilo_doval_1.jpeg",
}
data.upload_youtube()
