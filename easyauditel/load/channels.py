# Basic imports
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType, IntegerType, DoubleType, MapType, DateType, FloatType, StructType, StructField, ArrayType
from pyspark.sql import DataFrame
import datetime
from datetime import timedelta
from datetime import date
from functools import reduce

from easyauditel.utils import get_date_intervals, get_year_month_intervals
from easyauditel.utils import read_csv_from_s3

def read_channels(spark, 
                  start_date, end_date,
                  date_format,
                  s3_channels_path_list=["s3://dl-agb-prod/dl_rti/dl_agb_tstnazstd/daily", "s3://dl-agb-prod/dl_rti/dl_agb_tstlocstd/daily", "s3://dl-agb-prod/dl_rti/dl_agb_tstcircstd/daily"], 
                 ):

    '''
    Funzione che legge il dato elementare dei canali (channels) nell'intervallo di tempo desiderato
    e lo carica su dataframe Spark.
    

    Args:
        start_date (str): data iniziale
        end_date (str): data finale
        date_format (str): formato delle date passate alla funzione
        s3_channels_path_list (list(str)): lista dei path S3 relativi al tracciamento canali 

    Returns:
        channels (Spark df): dataframe con i dati sui channels nell'arco temporale desiderato
        
    '''

    year_month_intervals = get_year_month_intervals(start_date, end_date, date_format)
    date_intervals = get_date_intervals(start_date, end_date, date_format, '%Y%m%d')
    
    channels_dict = {}

    # Ciclo sulle coppie anno, mese:
    for i, (y, m) in enumerate(year_month_intervals):
        # Ciclo sui diveri path disponibili:
        for j, ch_path in enumerate(s3_channels_path_list):
            # Lettura e caricamento nel dictionary:
            channels_dict[str(j)+str(m)+str(y)] = read_csv_from_s3(spark, f'{ch_path}/year={y}/month={str(m).zfill(2)}/', sep='|',
                                                header=False)        
    
    # Unione dei dataframe:
    channels = reduce(DataFrame.union, channels_dict.values())
    
    # Rinomina campi e selezione dei canali distinti:
    channels = channels.withColumn("EMITTENTE_CODE", F.substring("_c0", 1, 4)) \
        .withColumn("CHANNEL", F.rtrim(F.substring("_c0", 5, 100))) \
        .drop("_c0") \
        .distinct() \
        .groupBy("EMITTENTE_CODE") \
        .agg(F.first("CHANNEL").alias("CHANNEL"))
        
    return channels