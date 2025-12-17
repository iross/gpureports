def calculate_linear_trend(dates, values):
    """Calculate linear trend statistics for the data."""
    # Convert dates to ordinal numbers for regression
    date_nums = [datetime.strptime(date, "%Y-%m-%d").toordinal() for date in dates]

    # Pure Python implementation (works without numpy/scipy)
    n = len(dates)
    if n < 2:
        return None

    # Calculate means
    x_mean = sum(date_nums) / n
    y_mean = sum(values) / n

    # Calculate slope and intercept
    numerator = sum((date_nums[i] - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((date_nums[i] - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return None

    slope = numerator / denominator
    intercept = y_mean - slope * x_mean

    # Calculate correlation coefficient
    x_var = sum((date_nums[i] - x_mean) ** 2 for i in range(n))
    y_var = sum((values[i] - y_mean) ** 2 for i in range(n))

    if x_var == 0 or y_var == 0:
        r_value = 0
    else:
        r_value = numerator / (x_var * y_var) ** 0.5

    r_squared = r_value**2
    trend_line = [slope * x + intercept for x in date_nums]

    # Set p_value and std_err to None for pure Python
    p_value = None
    std_err = None

    # Calculate trend per day and per month
    trend_per_day = slope
    trend_per_month = slope * 30.44  # Average days per month
    trend_per_year = slope * 365.25

    return {
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "r_value": r_value,
        "p_value": p_value,
        "std_err": std_err,
        "trend_line": trend_line,
        "trend_per_day": trend_per_day,
        "trend_per_month": trend_per_month,
        "trend_per_year": trend_per_year,
        "date_nums": date_nums,
    }
