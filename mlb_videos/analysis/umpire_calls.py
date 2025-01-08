import pandas as pd
import swifter

FILTERED_DESCRIPTIONS = ["called_strike", "ball"]
REQUIRED_COLUMNS = [
    "description",
    "sz_bot",
    "sz_top",
    "plate_x",
    "plate_z",
    "stand",
]
RETURN_COLUMNS = [
    "horizontal_miss_type",
    "horizontal_miss",
    "vertical_miss_type",
    "vertical_miss",
    "total_miss_type",
    "total_miss",
    "miss_delta_win_exp_impact",
]
DEFAULT_RETURN = [None, 0.00, None, 0.00, None, 0.00, 0.00]

AVG_ERROR = 0.25
SAMPLE = 10000
USE_ERROR_SAMPLE = False
PROB_REQ = 90

BALL_RADIUS = 0.12
ZONE_RADIUS = 0.83
ZONE_WIDTH = ZONE_RADIUS * 2
X1 = -ZONE_RADIUS
X2 = X1 + ZONE_WIDTH


def _umpire_calls(p: pd.Series) -> tuple:
    Y1 = p.sz_bot - BALL_RADIUS
    Y2 = p.sz_top - BALL_RADIUS
    X = p.plate_x
    Y = p.plate_z

    if p.description == "called_strike":
        if X < X1:
            horizontal_miss = round((X1 - X) * 12.00, 2)
            horizontal_miss_type = "inside" if p.stand == "R" else "outside"
        elif X > X2:
            horizontal_miss = round((X - X2) * 12.00, 2)
            horizontal_miss_type = "outside" if p.stand == "R" else "inside"
        else:
            horizontal_miss = 0.00
            horizontal_miss_type = None

        if Y < Y1:
            vertical_miss = round((Y1 - Y) * 12.00, 2)
            vertical_miss_type = "low"
        elif Y > Y2:
            vertical_miss = round((Y - Y2) * 12.00, 2)
            vertical_miss_type = "high"
        else:
            vertical_miss = 0.00
            vertical_miss_type = None

        if (horizontal_miss or vertical_miss) > 0:
            if p.inning_topbot == "Top" and p.delta_home_win_exp > 0:
                miss_delta_win_exp_impact = abs(p.delta_home_win_exp)
            elif p.inning_topbot == "Bot" and p.delta_home_win_exp < 0:
                miss_delta_win_exp_impact = abs(p.delta_home_win_exp)
            else:
                miss_delta_win_exp_impact = 0.00
        else:
            miss_delta_win_exp_impact = 0.00

    elif p.description == "ball":
        if (X1 < X and X < X2) and (Y1 < Y and Y < Y2):
            if X <= 0:
                horizontal_miss = round((X - X1) * 12.00, 2)
                horizontal_miss_type = "inside" if p.stand == "R" else "outside"
            elif X >= 0:
                horizontal_miss = round((X2 - X) * 12.00, 2)
                horizontal_miss_type = "outside" if p.stand == "R" else "inside"
            else:
                horizontal_miss = 0.00
                horizontal_miss_type = None

            if Y <= (Y1 + ((Y2 - Y1) / 2)):
                vertical_miss = round((Y - Y1) * 12.00, 2)
                vertical_miss_type = "low"
            elif Y >= (Y1 + ((Y2 - Y1) / 2)):
                vertical_miss = round((Y2 - Y) * 12.00, 2)
                vertical_miss_type = "high"
            else:
                vertical_miss = 0.00
                vertical_miss_type = None

            if horizontal_miss or vertical_miss > 0:
                if p.inning_topbot == "Bot" and p.delta_home_win_exp > 0:
                    miss_delta_win_exp_impact = abs(p.delta_home_win_exp)
                elif p.inning_topbot == "Top" and p.delta_home_win_exp < 0:
                    miss_delta_win_exp_impact = abs(p.delta_home_win_exp)
                else:
                    miss_delta_win_exp_impact = 0.00
            else:
                miss_delta_win_exp_impact = 0.00

        else:
            return tuple(DEFAULT_RETURN)

    total_miss = round(horizontal_miss + vertical_miss, 2)

    if horizontal_miss > 0 and vertical_miss > 0:
        total_miss_type = "both"
    elif horizontal_miss > 0:
        total_miss_type = horizontal_miss_type
    elif vertical_miss > 0:
        total_miss_type = vertical_miss_type
    else:
        total_miss_type = None

    return (
        horizontal_miss_type,
        horizontal_miss,
        vertical_miss_type,
        vertical_miss,
        total_miss_type,
        total_miss,
        miss_delta_win_exp_impact,
    )


def umpire_calls(df: pd.DataFrame) -> pd.DataFrame:
    df[RETURN_COLUMNS] = df.swifter.apply(
        lambda x: (
            DEFAULT_RETURN
            if any(pd.isnull(x[col]) for col in REQUIRED_COLUMNS)
            or x.description not in FILTERED_DESCRIPTIONS
            else (_umpire_calls(x))
        ),
        axis=1,
        result_type="expand",
    )
    return df
