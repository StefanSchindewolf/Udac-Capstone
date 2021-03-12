import pandas as pd
import numpy as np
import logging
import sys
import getopt
import botocore
import configparser
from datetime import datetime
from os.path import getsize
from nb_helpers import summarize_data, get_sas_definitions, read_sas_in_chunks, \
                        read_csv_print, print_stat, non_iso_date_change, change_nullables
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import pandas_udf


    

def trim_col_headers(df):
    """ Changes column headers from upper case to
        lower case and replaces inner spaces by an underscore.
        
        Returns: Dataframe with simple to read column headers"""
    
    for col in df.columns:
        changed_col = col.strip().lower().replace(' ', '_').replace('-', '_')
        df = df.withColumnRenamed(col, changed_col)
    print('Trimmed column headers')
    print(df.columns)
    return df


def change_type(df=None, fields=None, totype=None):
    """ Wraps around PySpark's `cast` function and adds a little output.
        
        Returns: Dataframe with changed types"""
    
    if ((totype is not None) and (fields is not None)):
        for column in fields:
            cur_type = df.schema[column]
            df = df.withColumn(column, F.col(column).cast(totype))
            new_type = df.schema[column]
            print('Done, switched column {} from {} to {}' \
                  .format(column, cur_type.dataType, new_type.dataType))
        print(df.select(fields).show(3))
        return df
    else:
        return None


def sasdate_to_date(df=None, column_list=None):
    """ Changes SAS date into ISO date format by calculating the number of days
        from the SAS epoch start date (which is 1st Jan 1960)
        
        Returns: dataframe with iso-formatted dates"""
    
    from pyspark.sql.functions import date_add
    
    # SAS has its own epoch, which we add temporarily as a column
    df = df.withColumn('epoch_start', F.lit("01-01-1960 00:00:00"))
    df = df.withColumn('epoch_start', F.to_date(F.col('epoch_start'), "dd-M-yyyy"))
    
    # Check each column and convert double to int, then add this int to epoch_start
    for column in column_list:
        df = df.withColumn(column, F.col(column).cast('int'))
        stm = 'date_add(epoch_start, {})'.format(column)
        df = df.withColumn(column, F.expr(stm))
    return df


def weighted_average(df=None, mean_column=None, weight_column=None, agg_level=None, session=None):
    """ Calculate weighted average for a dataframe.
        You need to provide the column which stores the average values
        and the column storing the weights.
        You can specify an aggregation level which is a column by which the
        data will be grouped (resulting in weighted averages for each group)

        Returns: Dataframe with weighted average column
        """
    
    # Reduce to required columns:
    df = df.select([mean_column, weight_column, agg_level]) \
            .withColumnRenamed(mean_column, 'mean_col') \
            .withColumnRenamed(weight_column, 'weight_col') \
            .withColumnRenamed(agg_level, 'agg_col')

    # Sum up weight column grouped by aggregation level
    sum_of_weights = df.select('agg_col', 'weight_col').groupBy('agg_col').sum()
    sum_of_weights = sum_of_weights.withColumnRenamed('sum(weight_col)', 'sum_weights')

    # Calculate product of individual means multiplied by weights
    mean_times_weight = df.withColumn('mean_weight_product', (F.col('mean_col') * (F.col('weight_col'))))
    
    # Sum up all mean values per agg level
    sum_of_means = mean_times_weight.select(['agg_col', 'mean_weight_product']).groupBy('agg_col').sum()
    sum_of_means = sum_of_means.withColumnRenamed('sum(mean_weight_product)', 'sum_of_meanpr')

    # Divide aggregated means by total weights per agg level
    weighted_mean = sum_of_means.join(sum_of_weights, 'agg_col')
    weighted_mean = weighted_mean.withColumn('weighted_avg', (F.col('sum_of_meanpr') / F.col('sum_weights')))
    
    # Send result back
    return_df = session.createDataFrame(weighted_mean.select(['agg_col', 'weighted_avg']).collect())
    return return_df


def create_spark_session(write_to_s3, config):
    """ This function creates a spark session handler
        Returns: SparkSession
    """
    
    if write_to_s3 is False:
        spark = SparkSession.builder \
                        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.1.0-s_2.11") \
                        .enableHiveSupport() \
                        .getOrCreate()
        print(datetime.now(), ' STEP 1 - CREATE SPARK SESSION - DONE')
    else:
        aws_key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
        aws_secret = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
        print('Reading credentials : {} {}'.format(aws_key, aws_secret))
        spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2') \
        .enableHiveSupport() \
        .getOrCreate()
        sc = spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
        sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3a.enableV4", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        print(datetime.now(), ' STEP 1 - CREATE SPARK SESSION WITH S3 CONTEXT - DONE')
    return spark


def get_staging_immigration(session, config_handler, sample_size):
    """ Reads data from the SAS datasource provided in the config file. At this
        stage we already perform some transformations which will make our script
        less complicated down the line.
        
        Returns: Immigration dataframe
        """

    # Get file locations and import data
    config_handler.read('dl.cfg')
    imm_file = config_handler.get('SOURCE', 'IMM_FILE')
    return_df = session.read \
        .format('com.github.saurfang.sas.spark') \
        .option("inferSchema","true") \
        .load(imm_file)
    imm_rows = return_df.count()
    print('Reading ', imm_rows , ' lines from ', imm_file)

    # TFA-1 Change data types
    int_columns = ['cicid','i94yr', 'i94mon', 'i94cit', 'i94res', \
                   'arrdate', 'depdate', 'i94mode', 'i94bir', \
                   'i94visa', 'count', 'biryear']
    return_df = change_type(return_df, int_columns, 'int')
    return_df = change_type(return_df, ['admnum'], 'bigint')

    # TFA-5 Change SAS date
    sas_date_columns = ['arrdate', 'depdate']
    return_df = sasdate_to_date(return_df, sas_date_columns)

    # TFA-6 Change non-iso date
    imm_non_iso_dates = {'dtaddto': 'MMddyyyy', 'dtadfile': 'yyyyMMdd'}
    return_df = non_iso_date_change(return_df, imm_non_iso_dates)

    not_null_fields = ['cicid', 'admnum']
    return_df = return_df.dropna(subset=not_null_fields)
    if sample_size is not None:
        return_df = return_df.sample(sample_size)
    print(return_df.show(3))
    print(datetime.now(), ' STEP 2a - READ IMMIGRATION DATA - DONE')
    return return_df
    

def get_staging_demographics(session, config_handler):
    """ Read demographics data from provided datasource in the config file
        and trim the headers.
        
        Returns: Demographics Dataframe
        """

    # Get file locations and import data
    config_handler.read('dl.cfg')
    dem_file = config_handler.get('SOURCE', 'DEM_FILE')
    
    # TFA-3 - Read with another separator
    return_df = session.read.csv(dem_file, header=True, sep=';')
    
    # TFA-4 - Trim column headers
    return_df = trim_col_headers(return_df)
    
    # TFA-2 - Change data types
    float_columns = ['median_age', 'average_household_size', 'count' ]
    int_columns = ['male_population', 'female_population', \
               'total_population', 'number_of_veterans', 'foreign_born']
    return_df = change_type(return_df, float_columns, 'double')
    return_df = change_type(return_df, int_columns, 'int')
    
    dem_rows = return_df.count()
    print('Reading ', dem_rows, ' lines from ', dem_file)
    print(return_df.show(3))
    print(datetime.now(), ' STEP 2c - READ DEMOGRAPHICS DATA - DONE')
    return return_df


def get_staging_airports(session, config_handler):
    """ Reads data from provided file name in the config file and casts
        the airport elevation data to float.
        
        Returns: Airport Dataframe
        """
    
    # Read data from file
    config_handler.read('dl.cfg')
    air_file = config_handler.get('SOURCE', 'AIR_FILE')
    return_df = session.read.csv(air_file, header=True)
    air_rows = return_df.count()
    print('Reading ', air_rows, ' lines from ', air_file)

    # TFA-2 Change datatypes
    float_columns = ['elevation_ft']
    return_df = return_df.withColumn('elevation_ft', F.col('elevation_ft').cast('float'))
    print(return_df.show(3))
    print(datetime.now(), ' STEP 2b - READ AIRPORT DATA - DONE')
    return return_df


def process_demographics_data(df, session):
    """ Takes provided demographics staging dataframe and aggregates the data
        either on state level or city level.
        For data containing a mean value this means the function needs to
        calculate a weigthed average, while total population numbers are
        simply summed up.
        
        Returns: Dataframe for demographics dimension table
        """
    
    # Select ALL columns for this dimension table into dimension table
    dim_demographics = df.select('*').dropDuplicates()

    # Define grouping function per column
    cols_to_mean = 'average_household_size'
    cols_to_sum = ['state_code', 'male_population', 'female_population', \
                   'total_population', 'number_of_veterans', 'foreign_born']

    # Calculate weighted average per state (arithmetic average not applicable)
    weighted_mean_df = weighted_average(dim_demographics, cols_to_mean, 'total_population', 'state_code', session)
    
    # Aggregate summary columns using groupby()
    summed_columns_df = dim_demographics.select(cols_to_sum).groupBy('state_code').sum()
    
    # Join both results
    dim_demographics = summed_columns_df.join(weighted_mean_df, weighted_mean_df.agg_col == summed_columns_df.state_code)
    
    # Drop irrelevant columns
    dim_demographics = dim_demographics.drop('agg_col') \
                        .withColumnRenamed('sum(male_population)', 'male_population') \
                        .withColumnRenamed('sum(female_population)', 'female_population') \
                        .withColumnRenamed('sum(total_population)', 'total_population') \
                        .withColumnRenamed('sum(number_of_veterans)', 'number_of_veterans') \
                        .withColumnRenamed('sum(foreign_born)', 'foreign_born') \
                        .withColumnRenamed('weighted_avg', 'average_household_size')
    
    dim_demographics.name = 'dim_demographics'
    print(dim_demographics.show(3))
    print(datetime.now(), ' STEP 3b - DEMOGRAPHICS DIMENSION TABLE - DONE')
    return dim_demographics


def process_airport_data(df):
    """ Selects required fields for dimension table and transforms the data
        by removing non-relevant columns and rows and splitting coordinates
        into two columns for latitude and longitude.
        
        Returns: Airport Dimension Dataframe
        """
    
    # Read all available airport codes from the airport dataframe
    airports_fields = ['iata_code', 'name', 'municipality', 'iso_region', \
                   'iso_country', 'coordinates']
    dim_airports = df.filter(df.iata_code.isNotNull()).select(airports_fields)

    # Split the coordinates column into latitude and longitude, remove spaces
    dim_airports = dim_airports.withColumn('latitude', F.split(dim_airports['coordinates'], ',').getItem(0)).withColumn('longitude', F.split(dim_airports['coordinates'], ',').getItem(1))
    dim_airports = dim_airports \
            .withColumn('iata_code', F.trim(F.col('iata_code'))) \
            .withColumn('latitude', F.trim(F.col('latitude'))) \
            .withColumn('longitude', F.trim(F.col('longitude')))
    dim_airports = dim_airports.drop('coordinates')
    
    dim_airports.name = 'dim_airports'
    print(dim_airports.show(3))
    print(datetime.now(), ' STEP 3a - AIRPORT DIMENSION TABLE - DONE')
    return dim_airports



def process_time_data(df, session):
    """ This function takes all date columns (defined below) with their distinct entries.
        Then for all dates it stacks given columns upon one another and
        calculates from those dates the days, weeks, months and the year.

        Returns: Dataframe with time dimension table
        """
    # Define the relevant fields where to collect date entries for time dimension table
    dim_time_columns = ['arrdate', 'depdate', 'dtaddto', 'dtadfile']
    
    # Reduce given df to the provided columns and remove duplicates
    dates_in_col = df.select(dim_time_columns).dropDuplicates()

    # Create a dataframe containing content of the 1st data column
    dim_time = session.createDataFrame(dates_in_col.select(dim_time_columns[0]).distinct().collect())

    # Iterate through the other columns and unionize them with the first one (by one :-)
    for next_col in dim_time_columns[1:]:
        temp_df = session.createDataFrame(dates_in_col.select(next_col).collect())
        dim_time = dim_time.union(temp_df).distinct()

    # Calculate remaining colums from data in 1st column
    dim_time = dim_time.withColumnRenamed(dim_time_columns[0], 'datestamp')
    dim_time = dim_time \
                .withColumn('day_of_month', F.dayofmonth('datestamp')) \
                .withColumn('day_of_year', F.dayofyear('datestamp')) \
                .withColumn('week', F.weekofyear('datestamp')) \
                .withColumn('month', F.month('datestamp')) \
                .withColumn('year', F.year('datestamp')) \
                .dropDuplicates()
    dim_time.name = 'dim_time'
    print(dim_time.show(3))
    print(datetime.now(), ' STEP 3d - TIME DIMENSION TABLE - DONE')
    return dim_time


def process_status_data(imm_df):
    """ This function receives the immigration dataframe and selects
        the status flags.
        Each status flag combination receives an indivdual key by which
        the fact table can be grouped later on.
        
        Returns: Dataframe with status flags and key
        """
    # Those are the relevant columns
    status_columns = ['entdepa', 'entdepd', 'entdepu', 'matflag']
    dim_status = imm_df.select(status_columns) \
                    .dropna() \
                    .dropDuplicates() \
                    .withColumn("status_flag_id", \
                                F.monotonically_increasing_id())
    dim_status.name = 'dim_status'
    print(dim_status.show(3))
    print(datetime.now(), ' STEP 3c - STATUS DIMENSION TABLE - DONE')
    return dim_status


def process_immigration_data(imm_df, dim_status):
    """ Improves data quality, selects fields for fact table and orders
        dataframe columns more logically.
        
        Returns: Dataframe with immigration fact table
        """
    
    # List of columns which are used to join with dim_status
    join_columns = ['entdepa', 'entdepu', 'entdepd', 'matflag']

    # Join individual flag combinations with the status key from dim_status
    fact_df = imm_df.join(dim_status, join_columns, 'leftouter')

    # Rename columns, cast values (if not yet done so)
    immigration_facts = fact_df.withColumn("cicid", F.col("cicid").cast("integer")) \
                        .withColumn('status', F.col('status_flag_id')) \
                        .withColumnRenamed('admnum', 'adm_number') \
                        .withColumnRenamed('arrdate','arrival_dt') \
                        .withColumnRenamed('depdate','departure_dt') \
                        .withColumn("cit_country", F.col("i94cit").cast("integer")) \
                        .withColumn("res_country", F.col("i94res").cast("integer")) \
                        .withColumnRenamed("i94port", "airport") \
                        .withColumn("transport_mode", F.col("i94mode").cast("integer")) \
                        .withColumnRenamed("i94addr", "state") \
                        .withColumn("age", F.col("i94bir").cast("integer")) \
                        .withColumn("visatype", F.col("i94visa").cast("integer")) \
                        .drop('i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', \
                              'depdate', 'count', 'admnum', 'i94mode','i94bir' \
                              'i94bir', 'dtadfile', 'epoch_start', 'status'
                              'dtadfile', 'visapost', 'dtaddto', 'insnum', 'i94visa')

    # Just an info: those columns are not used anymore
    imm_df_ommitted_columns = ['i94yr', 'i94mon', 'dtadfile', 'epoch_start', 'status'
                              'dtadfile', 'visapost', 'dtaddto', 'insnum', 'i94visa']
    
    # These are the fact tables columns in proper order
    immigration_fact_fields = ['cicid', 'status', 'adm_number', 'transport_mode', 'airport', \
                               'state', 'arrival_dt', 'departure_dt', 'airline', \
                               'fltno', 'visatype', 'age', 'gender', 'res_country', \
                               'cit_country', 'occup']
    
    # Select the columns and return the dataframe
    immigration_facts = immigration_facts.select(immigration_fact_fields)
    immigration_facts.name = 'immigration_facts'
    print(immigration_facts.show(3))
    print(datetime.now(), ' STEP 3e - IMMIGRATION FACT TABLE - DONE')
    return immigration_facts


def write_to_parquet(df_out, location, name, s3loc):
    """ Writes provided dataframe content to parquet files
        Use "location" variable to specify folder/sub-folders
        and "name" for the parquet filename.
        Optionally: provide an S3 bucket, the function will then
        write to the S3 location

        Returns: Nothing
    """
    # Put together an output location link
    location = s3loc + location + name
    try:
        print('Trying to write {} to location: {}'.format(name, location))
        df_out.write.parquet(location, mode='overwrite', compression='gzip')
        print('Done writing')
    except Exception as e:
        print(datetime.now(), ': Writing output files failed: ', e)



def data_quality_check_01(stage_df, transformed_df, stage_column, transformed_column):
    """ Checks data quality of the transformed data by
        * counting duplicate values in columns supposed to contain unique values only
        * counting input and output values to check how many lines were transformed
        
        Returns: Nothing, read output in Spark log
        """
    
    # Count values before, after
    stage_count = stage_df.count()
    trans_count = transformed_df.count()
    print('DATA QUALITY CHECK: Counting values of dataframe {}:'.format(transformed_df.name))
    print('Source file had {} entries, resulting table has: {} entries ({})'.format(stage_count, trans_count, (stage_count - trans_count)))
    
    # Count distinct values in primary key column
    stage_uniq = stage_df.select(stage_column).dropDuplicates().count()
    trans_uniq = transformed_df.select(transformed_column).dropDuplicates().count()
    print('DATA QUALITY CHECK: Count unique values of dataframe {} for columns {} and {}:'.format(transformed_df.name, stage_column, transformed_column))
    print('Stage column {} had {} unique entries'.format(stage_column, stage_uniq))
    print('Transformed column {} had {} unique entries'.format(transformed_column, trans_uniq))
    if stage_uniq == trans_uniq:
        print('RESULT OK: Number of private key entries matches unique count ({})'.format(trans_uniq))
    else:
        print('WARNING: Private key column has duplicate entries ({})'.format(trans_count-trans_uniq))


def data_quality_check_02(fact_df, join_df, fact_col, join_col):
    """ Check how many lines can be joined on the given colum and count
        the resulting lines and the missed ones.
        
        This is to check if airport codes should be updated in the airports file
        in order to improve join hits for both tables
        
        Returns: Nothing, only log output
        """
    print('DATA QUALITY CHECK: Check how many dimension entries in table {} \
    can be joined with facts in fact table {} using columns {} and {}'.format(join_df.name, fact_df.name, join_col, fact_col))
    # Read the first column of fact table AND the given join column
    fact_df = fact_df.select([fact_df.columns[0], fact_col]).distinct()
    fact_df = fact_df.withColumn(fact_col, F.trim(F.col(fact_col)))

    # Join with dimension table and set "null" if no match
    leftouter_set = fact_df.join(join_df, fact_df[fact_col] == join_df[join_col], 'leftouter')
    
    # Count results
    matching = leftouter_set.select(fact_col).filter(F.col(fact_col).isNotNull()).count()
    not_matching = leftouter_set.select(fact_col).filter(F.col(fact_col).isNull()).count()
    print('Successfully joined lines: {} \t\t\tJoin failed on {} lines'.format(matching, (not_matching)))


def data_load_redshift():
    pass


def create_resource(KEY, SECRET, TYPE, config_handler):
    """ Create a resource for AWS specified as in TYPE (e.g. S3)
        Returns: AWS Resource"""
    config_handler.read('dl.cfg')
    AWS_ACCESS_KEY_ID = config_handler.get('AWS', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config_handler.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    try:
        print(datetime.now(), ': Setting up resource for ', TYPE)
        aws_cli = boto3.client(TYPE,
                               region_name='us-west-2',
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                               )
    except Exception as e:
        print(datetime.now(), ': FAILED creating resource: ', e)
    return aws_cli


def main():
    """ Main function does the following actions:
        1. Create a Spark Session
        2. Retrieve input data from different sources
            -- each source will get a separate function
        3. Process the data and store the result to S3
        4. Import data into Redshift
        """

    # For testing purposes you may run this script to work just on a sample
    # just run `etl.py --sample-size 0.1` and it will take a 10% sample of immigration data
    try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv, 's:', ['sample-size=', 'no-qa-check', 's3-store'])
        print(f"Name of the script        : {sys.argv[0]}")
        print(f"Arguments of the script   : {sys.argv[1:]}")
    except getopt.GetoptError as err:
        print('Option not recognized: ',err)  # will print something like "option -a not recognized"
        print(f"Arguments of the script   : {sys.argv[1:]}")
        sys.exit(2)

    sample_size = None
    write_to_s3 = False
    qa_check = True
    for o, p in opts:
        if o in ('-s', '--sample-size'):
            sample_size = float(p)
            print('Setting sample size to: {}'.format(p))
        if o in ('-aws', '--s3-store'):
            write_to_s3 = True
            print('S3 was chosen as data store location, will read link from dl.cfg')
        if o in ('-q', '--no-qa-check'):
            qa_check = False
            print('QA Checks are turned "OFF"')
        else:
            print('No command line arguments given, using default values')

    config = configparser.ConfigParser()
    # Get external configuration
    config.read('dl.cfg')
    parquet_folder = config.get('AWS', 'PARQUET_FOLDER')
    s3bucket = config.get('AWS', 'S3_BUCKET')
    
    # Step 1 - Spark Setup
    session = create_spark_session(write_to_s3, config)

    # Step 2 - Download Source Data
    imm_df = get_staging_immigration(session, config, sample_size)
    air_df = get_staging_airports(session, config)
    dem_df = get_staging_demographics(session, config)

    # Step 3
    #    Process Airport Data
    dim_airports = process_airport_data(air_df)

    #    Process demographics data
    dim_demographics = process_demographics_data(dem_df, session)
    
    #    Create Status dimension table
    dim_status = process_status_data(imm_df)
    
    #    Create time dimension table
    dim_time = process_time_data(imm_df, session)
    
     #    Process Immigration Data
    immigration_facts = process_immigration_data(imm_df, dim_status)

    # Step 4 - Data Quality check
    if qa_check is True:
        data_quality_check_01(imm_df, immigration_facts, 'cicid', 'cicid')
        data_quality_check_01(air_df, dim_airports, 'iata_code', 'iata_code')
        data_quality_check_01(dem_df, dim_demographics, 'state_code', 'state_code')
        data_quality_check_01(imm_df, dim_time, 'arrdate', 'datestamp')
        data_quality_check_01(imm_df, dim_time, 'depdate', 'datestamp')
        data_quality_check_01(imm_df, dim_time, 'dtaddto', 'datestamp')
        data_quality_check_01(imm_df, dim_time, 'dtadfile', 'datestamp')
    
        data_quality_check_02(immigration_facts, dim_airports, 'airport', 'iata_code')
        print(datetime.now(), ' STEP 4 - DATA QUALITY CHECKS - DONE')

    # Step 5 - Write to Parquet
    if write_to_s3 is False:
        for table in [immigration_facts, dim_demographics, dim_airports, dim_time, dim_status]:
            write_to_parquet(table, parquet_folder, table.name, '')
        print(datetime.now(), ' STEP 5 - WRITING TO PARQUET - DONE')
    else:
        for table in [immigration_facts, dim_demographics, dim_airports, dim_time, dim_status]:
            write_to_parquet(table, parquet_folder, table.name, s3bucket)
        print(datetime.now(), ' STEP 5 - WRITING TO PARQUET ON S3 - DONE')
    
    # Step 6 - Load to Redshift
    data_load_redshift()

    # Done, end of etl

if __name__ == "__main__":
    main()
