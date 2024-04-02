def date_filepath(ts):
    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")

    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")
    hour = ts.strftime("%H")
    minute = ts.strftime("%M")
    second = ts.strftime("%S")

    return f'{year}-{month}-{day}-{hour}-{minute}-{second}'