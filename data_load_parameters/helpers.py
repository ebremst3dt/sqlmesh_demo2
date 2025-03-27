from datetime import datetime

def start_of_year_minus_x_years(years_to_subtract: int) -> str:
    current_year = datetime.now().year
    year_minus_something = current_year - years_to_subtract
    return f"{year_minus_something}-01-01"
