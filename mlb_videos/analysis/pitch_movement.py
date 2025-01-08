import pandas as pd

_REQUIRED_COLUMNS = ["pfx_x", "pfx_z"]
_RETURN_COLUMNS = [
    "horizontal_break",
    "vertical_break",
    "total_break",
    "total_break_abs",
]


def _pitch_movement(r: pd.Series) -> tuple:
    return (
        (r.pfx_x * -12.00),
        (r.pfx_z * 12.00),
        ((r.pfx_x * -12.00) + (r.pfx_z * 12.00)),
        abs(((r.pfx_x * -12.00) + (r.pfx_z * 12.00))),
    )


def pitch_movement(df: pd.DataFrame) -> pd.DataFrame:
    df[_RETURN_COLUMNS] = df.swifter.apply(
        lambda x: (
            (0.00,) * len(_RETURN_COLUMNS)
            if any(pd.isnull(x[col]) for col in _REQUIRED_COLUMNS)
            else (_pitch_movement(x))
        ),
        axis=1,
        result_type="expand",
    )
    return df
