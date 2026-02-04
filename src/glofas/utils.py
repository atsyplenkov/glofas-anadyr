from datetime import date


def get_missing_years(last_data_date: date) -> list:
    """Get list of years that need to be downloaded."""
    today = date.today()
    current_year = today.year
    last_year = last_data_date.year

    missing_years = []

    if last_year < current_year:
        missing_years = list(range(last_year + 1, current_year + 1))
    elif last_year == current_year:
        days_since_last = (today - last_data_date).days
        if days_since_last > 7:
            missing_years = [current_year]

    return missing_years


def normalize_base_url(value: str) -> str:
    """Normalize the base URL for site asset references."""
    if value is None:
        return ""
    base = value.strip()
    if not base:
        return ""
    base = base.rstrip("/")
    if not base.startswith("/"):
        base = f"/{base}"
    if base == "/":
        return ""
    return base
