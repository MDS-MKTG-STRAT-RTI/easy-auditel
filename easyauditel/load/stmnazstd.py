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

def read_stmnazstd(spark, start_date: str, end_date: str,
                  date_format: str, fruition_type: str,
                  s3_stmnazstd_base_path='s3://dl-agb-prod/dl_rti/dl_agb_stmnazstd/daily', 
                  s3_class_ascolto_path='s3://dl-agb-prod/dl_rti/dl_agb_classificazioneascolto/daily', 
                  s3_class_device_path='s3://dl-agb-prod/dl_rti/dl_agb_classificazionedevice/daily'
                  ):
    '''
    Funzione che legge il dato elementare delle fruizioni (stmnazstd) nell'intervallo di tempo desiderato
    e lo carica su dataframe Spark.
    
    NB: nel corso degli anni, lo schema dei file Stmnazstd è cambiato. Alcune colonne sono state aggiunte,
    a seguito della produzione di nuovi file sulla classificazione dell'ascolto e dei device.

    Args:
        start_date (str): data iniziale
        end_date (str): data finale
        date_format (str): formato delle date passate alla funzione
        fruition_type (list): lista con le tipologie di fruizione da considerare (e.g. ['I', 'G'] per "Individual" e "Guest")
        s3_stmnazstd_base_path (str): path di S3 del file fianag (con le informazioni sulle fruizioni giornaliere)
        s3_class_ascolto_path (str): path di S3 del file classificazioneascolto (con la transcodifica dei codici di classificazione ascolto)
        s3_class_device_path (str): path di S3 del file classificazionedevice (con la transcodifica dei codici di classificazione device)

    Returns:
        stmnazstd (Spark df): dataframe con i dati di fruizione stmnazstd nell'arco temporale desiderato
        
    '''

    year_month_intervals = get_year_month_intervals(start_date, end_date, date_format)
    date_intervals = get_date_intervals(start_date, end_date, date_format, '%Y%m%d')
    

    stmnazstd_dict = {}
    class_ascolto_dict = {}
    class_device_dict = {}
    sociodemo = ["DATA","PANEL","PRG","TIPO","FAT_EXP","CS","CSE","STUDI","SESSO","ETA11","ABBONATO","ETA22","SKY CINEMA","SKY SPORT","SKY CALCIO",
             "MYSKY","NUOVECLASSIETA","REGIONE","CSE30","BROADBAND"]
    
    new_track = False
    for i, (y, m) in enumerate(year_month_intervals):
        # Inizio del nuovo tracciato: lettura dei file classificazioneascolto, classificazionedevice e stmnazstd
        if ( y >= 2022 and m >= 5) or ( y >= 2023 ):
            stmnazstd_dict[str(m)+str(y)] = read_csv_from_s3(spark, f'{s3_stmnazstd_base_path}/year={y}/month={str(m).zfill(2)}/', sep='|')
            class_ascolto_dict[str(m)+str(y)] = read_csv_from_s3(spark, f'{s3_class_ascolto_path}/year={y}/month={str(m).zfill(2)}/', sep='|')
            class_device_dict[str(m)+str(y)] = read_csv_from_s3(spark, f'{s3_class_device_path}/year={y}/month={str(m).zfill(2)}/', sep='|')
            new_track = True
        else:
        # Vecchio tracciato: lettura della sola stmnazstd
            stmnazstd_dict[str(m)+str(y)] = read_csv_from_s3(spark, f'{s3_stmnazstd_base_path}/year={y}/month={str(m).zfill(2)}/', sep='|')\
                .withColumn("TOTALE EMITTENTI", F.lit(None))\
                .withColumn("CLASSIFICAZIONE ASCOLTO", F.lit(None))\
                .withColumn("CLASSIFICAZIONE DEVICE", F.lit(None))
                
      
    stmnazstd = reduce(DataFrame.union, stmnazstd_dict.values())
    # Nel nuovo tracciato, il dataframe delle fruizione viene arricchito delle colonne provenienti dai nuovi file di classificazione
    if new_track:
        class_device = reduce(DataFrame.union, class_device_dict.values())
        class_ascolto = reduce(DataFrame.union, class_ascolto_dict.values())
        stmnazstd = stmnazstd.filter(F.col("TIPO").isin(fruition_type))\
            .filter(F.col("TIPO STM").isin('L','V')).filter(
            F.col("DATA").isin(date_intervals))\
            .join(class_device.withColumnRenamed("CODICE", "CLASSIFICAZIONE DEVICE")\
                          .withColumnRenamed("DESCRIZIONE", "DESCRIZIONE CLASSIFICAZIONE DEVICE"), ["DATA", "CLASSIFICAZIONE DEVICE"], "left")\
            .join(class_ascolto.withColumnRenamed("CODICE", "CLASSIFICAZIONE ASCOLTO")\
                          .withColumnRenamed("DESCRIZIONE", "DESCRIZIONE CLASSIFICAZIONE ASCOLTO"), ["DATA", "CLASSIFICAZIONE ASCOLTO"], "left")
    # Nel caso di vecchio tracciato, il dataframe viene comunque arricchito delle colonne provenienti dai nuovi file
    # di classificazione ma queste vengono popolate con None. 
    else:
        stmnazstd = stmnazstd.filter(F.col("TIPO").isin(fruition_type))\
            .filter(F.col("TIPO STM").isin('L','V')).filter(
            F.col("DATA").isin(date_intervals))\
            .withColumn("DESCRIZIONE CLASSIFICAZIONE DEVICE", F.lit(None))\
            .withColumn("DESCRIZIONE CLASSIFICAZIONE ASCOLTO", F.lit(None))
        
    return stmnazstd