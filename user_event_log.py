# user_event_log.py
"""
Logging helpers for Hyundai app.
Expects `connection` and `cursor` from tbl.py.

Make sure Log_user table has columns:
  user_id, Brand, Dealer, Location, Missing_file,
  Startdate, Enddate, Category, MissingPeriod, PeriodType, EventType, LogAddedOn
(If not, ALTER TABLE to add PeriodType and EventType)
"""

from typing import List, Any
import pandas as pd

try:
    from tbl import connection, cursor
except Exception as e:
    raise ImportError("Could not import connection/cursor from tbl.py") from e


def log_event(user_id: str,
              Brand: str = "",
              Dealer: str = "",
              Location: str = "",
              Missing_file: str = "",
              Startdate: str = "",
              Enddate: str = "",
              Category: str = "",
              MissingPeriod: str = "",
              period_type: str = "",   # <<-- snake_case
              event_type: str = "") -> bool:  # <<-- snake_case
    """
    Insert a single row into Log_user.
    """
    try:
        sql = """
        INSERT INTO Log_user
            (user_id, Brand, Dealer, Location, Missing_file,
             Startdate, Enddate, Category, MissingPeriod, period_type, EventType, LogAddedOn)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        params = (
            user_id,
            Brand,
            Dealer,
            Location,
            Missing_file,
            Startdate,
            Enddate,
            Category,
            MissingPeriod,
            period_type,
            event_type
        )
        cursor.execute(sql, params)
        connection.commit()
        return True
    except Exception as e:
        print(f"[user_event_log.log_event] Error: {e}")
        try:
            connection.rollback()
        except Exception:
            pass
        return False


def log_app_events(user_id: str,
                   start_date: Any,
                   end_date: Any,
                   select_categories: List[str],
                   missing_files: List[str],
                   validation_log_df: pd.DataFrame,
                   success: bool,
                   period_type: str):
    """
    High-level logging entrypoint.
    `period_type` must be passed here (e.g. "Day","Week","Month","Quarter","Year").
    """

    start_date_str = str(start_date)
    end_date_str = str(end_date)
    category_value = ",".join(select_categories) if select_categories else ""

    # Missing files
    for msg in missing_files or []:
        try:
            path, missing_part = msg.split(" - ", 1)
            brand, dealer, location = path.split("/", 2)
            missing_label = missing_part.replace("Missing:", "").strip()
        except Exception:
            brand = dealer = location = ""
            missing_label = msg

        log_event(
            user_id=user_id,
            Brand=brand,
            Dealer=dealer,
            Location=location,
            Missing_file=missing_label,
            Startdate=start_date_str,
            Enddate=end_date_str,
            Category=category_value,
            MissingPeriod="",
            period_type=period_type,    # <-- pass snake_case param
            event_type="FileMissing"
        )

    # Missing periods
    if validation_log_df is not None and not validation_log_df.empty:
        for _, row in validation_log_df.iterrows():
            log_event(
                user_id=user_id,
                Brand=row.get("Brand", "") or "",
                Dealer=row.get("Dealer", "") or "",
                Location=row.get("Location", "") or "",
                Missing_file=row.get("Missing In", "") or "",
                Startdate=start_date_str,
                Enddate=end_date_str,
                Category=category_value,
                MissingPeriod=row.get("Period", "") or "",
                period_type=period_type,
                event_type="PeriodMissing"
            )

    # Success (single global row)
    if success:
        log_event(
            user_id=user_id,
            Brand="ALL",
            Dealer="ALL",
            Location="ALL",
            Missing_file="",
            Startdate=start_date_str,
            Enddate=end_date_str,
            Category=category_value,
            MissingPeriod="",
            period_type=period_type,
            event_type="ProcessingSuccess"
        )
