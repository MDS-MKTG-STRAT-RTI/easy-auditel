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

from easyauditel.utils import date_time_to_timestamp, add_minute
from easyauditel.utils import read_csv_from_s3

# Definizione di UDF per la costruzione dell tabella enriched
calculate_ini_stm_auditel_udf = F.udf(date_time_to_timestamp, TimestampType())
add_minute_udf = F.udf(add_minute, TimestampType())

def enrich_fruitions_tot(spark, fianag, stmnazstd, channels, 
    s3_mapping_channels_path='s3://mediaset-mktg/profile-auditel/mapping/mapping_codici_auditel_canali.csv'):

    '''
    Funzione che unisce i dataframe relativi ai dati fianag, stmnazstd, channels e genera 
    un dataframe finale con tutte le informazioni rilevanti per ciascuna fruizione.

    Args:
        fianag (Spark df): dataframe Spark con dati fianag
        stmnazstd (Spark df): dataframe Spark con dati di fruizione stmnazstd
        channels (Spark df): dataframe Spark con dati channels distinti
        s3_mapping_channels_path (str): path di S3 con i file di transcodifica canali
    
    Returns:
        fruitions_enriched (Spark df): dataframe delle fruizioni "arrichito" con info da fianag e channels

    '''
    
    mapping_channels = read_csv_from_s3(spark, s3_mapping_channels_path).withColumn("CODE", F.lpad("CODE", 4, '0'))

    join_conditions = [
        fianag.DATA == stmnazstd.DATA,
        fianag.PANEL == stmnazstd.PANEL,
        fianag.PRG == stmnazstd.PRG]

    # Join tabelle
    fruitions = fianag.join(stmnazstd, join_conditions, 'left') \
        .drop(stmnazstd.DATA) \
        .drop(stmnazstd.PANEL) \
        .drop(stmnazstd.PRG) \
        
    channel_mapped_fruitions = fruitions.join(mapping_channels, fruitions.EMITTENTE == mapping_channels.CODE, 'left') \
        .select("DATA",
               F.concat(F.col("PANEL"), F.col("PRG")).alias("PANEL_PRG"),
               F.col("`FAT_EXP`").cast(DoubleType()).alias("FAT_EXP"),
               "EMITTENTE",
               "CANALE",
               F.concat(F.col("ORAI LIVE"), F.lit("00")).alias("INI_STM"),
               F.col("DURATA LIVE").cast("INTEGER").alias("DURATA_LIVE"),
                "TOTALE EMITTENTI", "CLASSIFICAZIONE ASCOLTO", "CLASSIFICAZIONE DEVICE", "BROADBAND",
               "DESCRIZIONE CLASSIFICAZIONE DEVICE","DESCRIZIONE CLASSIFICAZIONE ASCOLTO") \
        .filter(F.col("DURATA LIVE").isNotNull())\
        .withColumn("OTT", F.when(
            (F.col("DURATA_LIVE") >= 15) & (F.col("BROADBAND") == "1") & (F.col("EMITTENTE") == "9994") & (F.col("CLASSIFICAZIONE ASCOLTO").isin(["4","5"])) & (F.col("CLASSIFICAZIONE DEVICE").isin(["1","3","5"])), "y")\
            .otherwise("n"))
    
    desc_channel_fruitions = channel_mapped_fruitions \
        .join(channels, channel_mapped_fruitions.EMITTENTE == channels.EMITTENTE_CODE, 'left') \
        .withColumn("CANALE", F.when(F.col("CANALE").isNull(), F.col("CHANNEL")).otherwise(F.col("CANALE"))) \
        .withColumn("CANALE", F.when((F.col("CANALE").isNull()) & (F.col("EMITTENTE").isNotNull()), 'ALTRO').otherwise(F.col("CANALE")))

    fruitions_enriched = desc_channel_fruitions \
        .withColumn("INI_STM_ts", calculate_ini_stm_auditel_udf('DATA', 'INI_STM')) \
        .withColumn('END_STM_ts', add_minute_udf('INI_STM_ts', 'DURATA_LIVE')) \
        .withColumn("ASCOLTO_SPIEGATO", (F.col("FAT_EXP") * F.col("DURATA_LIVE")) / F.lit(1000)) \
        .select("DATA", "PANEL_PRG", "FAT_EXP", "CANALE", "EMITTENTE", "TOTALE EMITTENTI", "CLASSIFICAZIONE ASCOLTO", "CLASSIFICAZIONE DEVICE",
                "DESCRIZIONE CLASSIFICAZIONE DEVICE","DESCRIZIONE CLASSIFICAZIONE ASCOLTO",
                "INI_STM_ts", "END_STM_ts", "DURATA_LIVE", "ASCOLTO_SPIEGATO","OTT")
    
    return fruitions_enriched