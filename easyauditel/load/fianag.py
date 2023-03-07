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

def read_fianag(spark, start_date: str, end_date: str,
                  date_format: str, fruition_type,
                  s3_fianag_base_path='s3://dl-agb-prod/dl_rti/dl_agb_fianag/daily'
                  ):
    '''
    Funzione che legge il dato elementare del campione prodotto (fianag) nell'intervallo di tempo desiderato
    e lo carica su dataframe Spark.
    
    NB: nel corso degli anni, lo schema dei file Fianag è cambiato. Alcune colonne sono state aggiunte,
    altre hanno cambiato nome. Nel caso in cui il file letto non contenga una o più colonne di interesse
    (sociodemo), viene aggiunta artificialmente la colonna, riempita con valori null, per uniformare il tracciato.

    Args:
        start_date (str): data iniziale
        end_date (str): data finale
        date_format (str): formato delle date passate alla funzione
        fruition_type (list): lista con le tipologie di fruizione da considerare (e.g. ['I', 'G'] per "Individual" e "Guest")
        s3_fianag_base_path (str): path di S3 del file fianag (con le informazioni sui panelisti prodotti giornalmente)

    Returns:
        fianag (Spark df): dataframe con i dati fianag nell'arco temporale desiderato

    '''

    year_month_intervals = get_year_month_intervals(start_date, end_date, date_format)
    date_intervals = get_date_intervals(start_date, end_date, date_format, '%Y%m%d')
    
    fianag_dict = {}
    sociodemo = ["DATA","PANEL","PRG","TIPO","FAT_EXP","CS","CSE","STUDI","SESSO","ETA11","ABBONATO","ETA22","SKY CINEMA","SKY SPORT","SKY CALCIO",
            "MYSKY","NUOVECLASSIETA","REGIONE","CSE30","BROADBAND"]
    
    # Ciclo sulle coppie anno, mese:
    for i, (y, m) in enumerate(year_month_intervals):
        
        # Lettura del file nella partizione
        df = read_csv_from_s3(spark, f'{s3_fianag_base_path}/year={y}/month={str(m).zfill(2)}/', sep='|')
        
        # Eventuale rinomina delle colonne che hanno cambiato nome nel tempo
        df = df\
            .withColumnRenamed("FAT.EXP", "FAT_EXP")\
            .withColumnRenamed("CSE3.0", "CSE30")
        
        if y<= 2019 and m<=9:
            df = df\
                .withColumnRenamed("ETA12", "ETA11")\
                .withColumnRenamed("ETA23", "ETA22")\
                .withColumnRenamed("SATFREE21", "SATFREE20")\
                .withColumnRenamed("SATFREE45", "SATFREE44")
        
        # Check sulle colonne esistenti nel file
        missing_cols = set(sociodemo) - set(df.columns)
        
        # Aggiunte delle colonne mancanti con valori None
        for col in missing_cols:
            df = df.withColumn(col, F.lit(None))    
        
        # Selezione delle colonne di interesse
        df = df.select(sociodemo)
        
        # Aggiunta del Dataframe al dizionario
        fianag_dict[str(m)+str(y)] = df
        
    
    # Unione dei dataframe 
    fianag = reduce(DataFrame.union, fianag_dict.values())
    
    # Filtre sulla tipologia di fruizioni
    fianag = fianag.filter(F.col("TIPO").isin(fruition_type))\
        .filter(F.col("DATA").isin(date_intervals))
    
    return fianag