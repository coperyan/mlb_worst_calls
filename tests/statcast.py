import os
import sys

sys.path.insert(
    0, r"C:\Users\ryanc\Documents\Dev\GitHub - coperyan\mlb-videos-dev-v2\mlb_videos"
)
os.chdir(r"C:\Users\ryanc\Documents\Dev\GitHub - coperyan\mlb-videos-dev-v2\mlb_videos")

from statcast import Statcast

test = Statcast()
test.search(
    start_date="2024-03-20",
    end_date="2024-11-15",
    descriptions=["called_strike"],
    # events=["home_run"], teams=["SF"]
)
test.umpire_calls()
df = test.get_df()
df = df.query("total_miss >= 3").reset_index(drop=True)
df = df.query("release_speed >= 65").reset_index(drop=True)
df = df.sort_values(by="total_miss", ascending=False)
df["total_miss_rank"] = 1
df["total_miss_rank"] = df["total_miss_rank"].cumsum()
df = df.reset_index(drop=True)
