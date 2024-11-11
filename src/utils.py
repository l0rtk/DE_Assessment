from datetime import datetime


def parse_date(date_str: str) -> datetime:
    """return date string in DD.MM.YYYY format"""
    return datetime.strptime(date_str, '%d.%m.%Y')