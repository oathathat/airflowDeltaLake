from pyspark.sql import DataFrame
from pyspark.sql.functions import col,abs,when,to_timestamp,unix_timestamp,lit
import warnings
import re 
def to_snake_case(text: str) -> str:
    '''
    Converts string to snake case
    Parameters:
    - text: string to be converted to snake case
    Return:
    - string in snake case
    '''
    #spaces and hyphens 
    text = re.sub(r"[\s-]+", "_", text)
    #Underscore before uppercase letters that follow a lowercase or number
    text = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", text)    
    return text.lower()

def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    '''
    Rename columns to snake case
    Parameters: 
    - df: DataFrame to be rename columns to snake case
    Return: 
    - DataFrame with snake case columns
    '''
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, to_snake_case(col_name))
    return df

def check_columns_exist(df: DataFrame, columns: list[str]) -> list[str]:
    '''
    Check if the specified columns exist in the DataFrame and return a list of existing columns.
    Parameters:
    - df: DataFrame to be checked
    - columns: list of columns to be checked
    Return:
    - list of existing columns
    '''
    existing_columns = [column for column in columns if column in df.columns]
    missing_columns = set(columns) - set(existing_columns)
    if missing_columns:
        warnings.warn(f"Warning: Columns not found and will be skipped: {', '.join(missing_columns)}")
    return existing_columns

# Utils function
def filter_valid_range(df: DataFrame,
                       columns_ranges: dict[str, tuple[int, int]]
                       ) -> DataFrame:
    '''
    Filter rows based on valid range
    Parameters:
    - df: DataFrame to be filtered
    - columns_ranges: dictionary of column names(key) and valid ranges(value)
    Return:
    - DataFrame with valid rows
    '''
    for column, (min_val, max_val) in columns_ranges.items():
        if column in df.columns:
            df = df.filter((col(column) >= min_val) & (col(column) <= max_val))
        else:
            warnings.warn(f"Warning: Column '{column}' not found and will be skipped.")
    return df

def filter_positive (df:DataFrame,columns:list[str]) -> DataFrame:
    '''
    Filter positive values
    Parameters:
    - df: DataFrame to be filtered
    - columns: list of columns to be filtered
    Return:
    - DataFrame with positive values
    '''
    columns = check_columns_exist(df, columns)
    for column in columns:
        df = df.filter(col(column) > 0)
    return df

def filter_iqr_outliers(df:DataFrame,columns:list[str]) -> DataFrame:
    '''
    Filter outliers using IQR
    Parameters:
    - df: DataFrame to be filtered
    - columns: list of columns to be filtered
    Return:
    - DataFrame with outliers filtered
    '''
    columns = check_columns_exist(df, columns)
    for column in columns:
        q1 = df.approxQuantile(column, [0.25], 0.05)[0]
        q3 = df.approxQuantile(column, [0.75], 0.05)[0]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))
    return df

def fill_na(df:DataFrame,columns:list[str],fill_value) -> DataFrame:
    '''
    Fill missing values
    Parameters:
    - df: DataFrame to be filled
    - columns: list of columns to be filled 
    - fill_value: value to be filled
    Return:
    - DataFrame with missing values filled
    '''
    columns = check_columns_exist(df, columns)
    fill_dict = {column: fill_value for column in columns}
    return df.fillna(fill_dict)

def correct_negative_values(df: DataFrame, columns: list[str]) -> DataFrame:
    '''
    Replace negative values with absolute values
    Parameters:
    - df: DataFrame
    - columns: list of columns 
    Return:
    - DataFrame with corrected negative values
    '''
    columns = check_columns_exist(df, columns)    
    for column in columns:
        df = df.withColumn(column, when(col(column) < 0, abs(col(column))).otherwise(col(column)))
    return df

def filter_time_in_range(df: DataFrame, 
                         time_col: str ='tpep_pickup_datetime',                         
                         date_range: tuple[str, str] = ('2024-01-01', '2024-01-31')
                         ) -> DataFrame:
    '''
    Filter Dataframe by time range
    Parameters:
    - df: DataFrame to be filter
    - time_col: column of timestamp to be filter
    - date_range: tuple of start date and end date
    Return:
    - DataFrame with valid rows
    '''
    return df.filter(
                (col(time_col) >= lit(date_range[0])) & 
                (col(time_col) <= lit(date_range[1]))
                )
    
def drop_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    '''
    Drop specified columns
    Parameters:
    - df: DataFrame to be dropped
    - columns: list of columns to be dropped
    Return:
    - DataFrame with dropped columns
    '''
    columns = check_columns_exist(df, columns)
    return df.drop(*columns)

# More Specific
def adjust_payment_type(df: DataFrame) -> DataFrame:
    '''
    Plus 1 to payment_type (In this case, shift payment_type from [0,5] to [1,6])
    Parameters:
    - df: DataFrame to be adjusted
    Return:
    - DataFrame with adjusted payment_type
    '''
    columns = ["payment_type"]
    columns = check_columns_exist(df,columns)
    for column in columns:
        df = df.withColumn(
            column, 
            when(col(column) < 0, None)
            .when(col(column) >= 0, col(column) + 1)
            .otherwise(None)
        )
    return df

def change_timestamp_format(df: DataFrame, 
                           pickup_col: str = 'tpep_pickup_datetime', 
                           dropoff_col: str = 'tpep_dropoff_datetime'                  
                            ) -> DataFrame:
    '''
    Create new column of timestamp from original timestamp columns
    Parameters:
    - df: DataFrame to be changed
    - pickup_col: column of pickup timestamp
    - dropoff_col: column of dropoff timestamp
    Return:
    - DataFrame with changed timestamp format
    '''
    if pickup_col in df.columns:
        df = df.withColumn(
            'pickup_datetime',
            to_timestamp(col(pickup_col),'yyyy-MM-dd HH:mm:ss')
        ).drop(pickup_col)        
    else:        
        warnings.warn(f"Warning: Column '{pickup_col}' not found and will be skipped.")    
    if dropoff_col in df.columns:
        df = df.withColumn(
            'dropoff_datetime',
            to_timestamp(col(dropoff_col),'yyyy-MM-dd HH:mm:ss')            
        ).drop(dropoff_col)
    else:        
        warnings.warn(f"Warning: Column '{dropoff_col}' not found and will be skipped.")  
    return df 
        
# enrichment function
def add_trip_duration(df: DataFrame, 
                      pickup_col: str = "pickup_datetime", 
                      dropoff_col: str = "dropoff_datetime"
                      ) -> DataFrame:
    '''
    Create new column of trip_duration(in minutes)
    Parameters:
    - df: DataFrame
    - pickup_col: column of pickup timestamp
    - dropoff_col: column of dropoff timestamp
    Return:
    - DataFrame with added trip_duration
    '''
    if pickup_col in df.columns and dropoff_col in df.columns:
        df = df.withColumn(
            "trip_duration",
            (unix_timestamp(col(dropoff_col)) - unix_timestamp(col(pickup_col))) / 60
        )
    else:
        warnings.warn(f"Warning: Columns '{pickup_col}' or '{dropoff_col}' not found and will be skipped.")    
    return df
    
def add_fare_per_mile(df: DataFrame, fare_col: str = "fare_amount", distance_col: str = "trip_distance") -> DataFrame:
    '''
    Create new column of fare_per_mile (fare_amount / trip_distance)
    Parameters:
    - df: DataFrame
    - fare_col: column of fare_amount
    - distance_col: column of trip_distance
    Return:
    - DataFrame with added fare_per_mile
    '''
    if fare_col in df.columns and distance_col in df.columns:
        df = df.withColumn(
            "fare_per_mile",
            when(col(distance_col) > 0, col(fare_col) / col(distance_col)).otherwise(0)
        )
    else:
        print(f"Warning: '{fare_col}' or '{distance_col}' column is missing. 'fare_per_mile' column not added.")
    
    return df