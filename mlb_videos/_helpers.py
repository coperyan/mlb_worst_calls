import os
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

from _constants import DATE_FORMAT, SEASON_DATES


def get_file_information(file: str) -> dict:
    return {
        "filename": os.path.basename(file),
        "filepath": file,
        "name": os.path.splitext(os.path.basename(file))[0],
        "path": os.path.dirname(file),
        "ext": os.path.splitext(file)[1],
    }


def update_nested_dict_by_key(in_dict, key, value):
    for k, v in in_dict.items():
        if key == k:
            in_dict[k] = value
        elif isinstance(v, dict):
            update_nested_dict_by_key(v, key, value)
        elif isinstance(v, list):
            for o in v:
                if isinstance(o, dict):
                    update_nested_dict_by_key(o, key, value)


def update_nested_dict_by_val(d: dict, old: str, new: str) -> dict:
    x = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = update_nested_dict_by_val(v, old, new)
        elif isinstance(v, list):
            v = update_nested_dict_by_val_list(v, old, new)
        elif isinstance(v, str):
            v = v.replace(old, new)
        x[k] = v
    return x


def update_nested_dict_by_val_list(l: list, old: str, new: str) -> list:
    x = []
    for e in l:
        if isinstance(e, list):
            e = update_nested_dict_by_val_list(e, old, new)
        elif isinstance(e, dict):
            e = update_nested_dict_by_val(e, old, new)
        elif isinstance(e, str):
            e = e.replace(old, new)
        x.append(e)
    return x


def update_nested_dict_multiple_keys(d: dict, **kwargs) -> dict:
    for k, v in kwargs.items():
        update_nested_dict_by_key(in_dict=d, key=k, value=v)
    return d


def remove_none_values(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none_values(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)(
            (remove_none_values(k), remove_none_values(v))
            for k, v in obj.items()
            if k is not None and v is not None
        )
    else:
        return obj


def get_date(
    days_ago: int = None,
    weeks_ago: int = None,
    months_ago: int = None,
    years_ago: int = None,
    fmt: str = DATE_FORMAT,
):
    if days_ago:
        return (datetime.now() - timedelta(days=days_ago)).strftime(fmt)
    elif weeks_ago:
        today = date.today()
        start_date = today + timedelta(-today.weekday() - 1, weeks=-weeks_ago)
        end_date = start_date + timedelta(days=6)

        return {
            "start_date": start_date.strftime(fmt),
            "end_date": end_date.strftime(fmt),
            "week_number": int(start_date.strftime("%W")),
            "year": int(start_date.strftime("%Y")),
        }
    elif months_ago:
        today = date.today()
        today_x_months_ago = today - relativedelta(months=x)
        first_day_x_months = today_x_months_ago.replace(day=1)
        today_x_months_ago_next = today - relativedelta(months=months_ago - 1)
        last_day_x_months = today_x_months_ago_next.replace(day=1) - timedelta(days=1)

        return {
            "start_date": first_day_x_months.strftime(fmt),
            "end_date": last_day_x_months.strftime(fmt),
            "month_name": last_day_x_months.strftime("%B"),
            "month_number": int(last_day_x_months.strftime("%m")),
            "year": int(last_day_x_months.strftime("%Y")),
        }
    elif years_ago:
        today = date.today()
        x_years_ago = today - relativedelta(years=years_ago)
        return x_years_ago.strftime(fmt)


def get_date_range(start_dt: str, end_dt: str) -> list:
    """Get Date Range

    Parameters
    ----------
        start_dt : str
        end_dt : str

    Returns
    -------
        list
            list of dates (str)
    """
    start_date = datetime.strptime(start_dt, DATE_FORMAT).date()
    end_date = datetime.strptime(end_dt, DATE_FORMAT).date()
    max_date = datetime.strptime(get_date(days_ago=1), DATE_FORMAT).date()

    dates = []
    iter_date = start_date

    while iter_date <= end_date:
        season_start, season_end = SEASON_DATES.get(iter_date.year)

        if iter_date < season_start:
            iter_date = season_start
        elif iter_date > season_end:
            iter_date, _ = SEASON_DATES.get(iter_date.year + 1)

        if iter_date > end_date or iter_date > max_date:
            break
        else:
            dates.append(iter_date.strftime(DATE_FORMAT))
            iter_date += timedelta(days=1)

    return dates


class DotDict(dict):
    """
    a dictionary that supports dot notation
    as well as dictionary access notation
    usage: d = DotDict() or d = DotDict({'val1':'first'})
    set attributes: d.val2 = 'second' or d['val2'] = 'second'
    get attributes: d.val2 or d['val2']
    """

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, dct):
        for key, value in dct.items():
            if hasattr(value, "keys"):
                value = DotDict(value)
            self[key] = value
