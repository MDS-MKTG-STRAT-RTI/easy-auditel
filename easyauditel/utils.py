# Basic imports
import datetime
from datetime import date
from datetime import timedelta

def read_csv_from_s3(spark, s3_path: str, sep=';', header=True):
    return spark.read.csv(s3_path, sep=sep, header=header)

def get_date_intervals(start_date: str, end_date: str, input_date_format: str, output_date_format: str):
    '''
    Funzione che accetta una data iniziale e una data finale, come stringhe, e restituisce una lista di
    date comprese tra queste, come stringhe, nel formato specificato.

    Args:
        start_date (str): data di inizio
        end_date (str): data di fine
        input_date_format (str): formato date input (es. '%Y-%m-%d')
        output_date_format (str): formato date output (es. '%Y%m%d')

    Returns:
        date_intervals (list[str]) : lista di date comprese tra start_date e end_date, nel formato specificato da output_date_format


    >>> get_date_intervals('2023-01-01', '2023-01-02', '%Y-%m-%d', '%Y%m%d')
    ['20230101','20230102']
    '''

    # Cast stringhe a datetime object
    sd = datetime.datetime.strptime(start_date, input_date_format)
    ed = datetime.datetime.strptime(end_date, input_date_format)

    # Generazione intervallo
    interval = [sd + datetime.timedelta(days=d) for d in range(0, 1 + (ed - sd).days)]
    # Conversione a stringhe
    date_intervals = [d.strftime(output_date_format) for d in interval]

    return date_intervals

def get_year_month_intervals(start_date: str, end_date: str, date_format: str):
    '''
    Funzione che accetta una data iniziale e una data finale, come stringhe, e restituisce un insieme di tuple
    (anno, mese), comprese tra le due date.

    Args:
        start_date (str): data di inizio
        end_date (str): data di fine
        date_format (str): formato date input (es. '%Y-%m-%d')

    Returns:
        year_month_set (set(date.year, date.month)) : insieme di tuple (anno, mese) comprese tra start_date e end_date

    >>> get_year_month_intervals('2023-01-01', '2023-02-01', '%Y-%m-%d')
    {(2023, 01), (2023, 02)}
    '''

    # Cast stringhe a datetime object
    sd = datetime.datetime.strptime(start_date, date_format)
    ed = datetime.datetime.strptime(end_date, date_format)

    # Generazione intervallo
    interval = [sd + datetime.timedelta(days=d) for d in range(0, 1 + (ed - sd).days)]

    # Generazione insieme
    year_month = [(d.year, d.month) for d in interval]
    year_month_set = set(year_month)

    return year_month_set

def date_time_to_timestamp(date, time):
    '''
    Funzione che accetta una data e un'ora in formato stringa and restituisce un timestamp 
    nel formato YYYY-MM-DD HH:MM:SS
    
    Args:
        date (str): data nel formato YYYYMMDD
        time (str): tempo nel formato HHMMSS
        
    Returns:
        normalized_timestamp (datetime): timestamp nel formato YYYY-MM-DD HH:MM:SS
    '''
    hour = int(time[:2])
    date_ = datetime.datetime.strptime(date, "%Y%m%d")
    if hour >= 24:
        normalized_time_str = str(hour - 24).zfill(2) + time[2:]
        normalized_date_str = (date_ + timedelta(days=1)).strftime("%Y%m%d")
        normalized_timestamp_str = f'{normalized_date_str}{normalized_time_str}'
        normalized_timestamp = datetime.datetime.strptime(normalized_timestamp_str, "%Y%m%d%H%M%S")
    else:
        normalized_timestamp = datetime.datetime.strptime(date + time, "%Y%m%d%H%M%S")

    return normalized_timestamp

def add_minute(date, durata):
    '''
    Funzione per aggiungere una durata in minuti ad una data
    
    Args:
        date (datetime): data
        durata (int): minuti da aggiungere
    
    Returns:
        new_date (date): nuova data con minuti aggiunti
    '''
    new_date = date + timedelta(minutes=durata)
    
    return new_date