import pandas as pd
import numpy as np
import os
from datetime import datetime
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, format="{time} | {level} | {message}")

def main():
    # Input and output file paths
    input_file = "validated_streaming.csv"
    output_raw_file = "train.csv"
    output_processed_file = "train_clean.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} does not exist")
        return
    
    # Read first 1,500,000 rows from the input file
    logger.info(f"Reading first 1,500,000 rows from {input_file}")
    try:
        df = pd.read_csv(input_file, nrows=1000001)
        logger.info(f"Successfully read {len(df)} rows")
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}")
        return
    
    # Save raw data to train.csv
    logger.info(f"Saving raw data to {output_raw_file}")
    df.to_csv(output_raw_file, index=False)
    logger.info(f"Successfully saved {len(df)} rows to {output_raw_file}")
    
    # Process data using logic from training_data_stream.py
    logger.info("Processing data...")
    try:
        processed_df = process_data(df)
        logger.info(f"Successfully processed data, resulting in {len(processed_df)} rows")
        
        # Save processed data to train_clean.csv
        processed_df.to_csv(output_processed_file, index=False)
        logger.info(f"Successfully saved processed data to {output_processed_file}")
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return

def process_data(df):
    """Process data using logic from training_data_stream.py"""
    logger.info(f"Processing data with {len(df)} records")
    
    # Clone the dataframe to avoid modifying the original
    processed_df = df.copy()
    
    # Rename column and convert timestamp
    processed_df.rename(columns={"event_time": "event_timestamp"}, inplace=True)
    
    # Check if event_timestamp is a string or integer
    if pd.api.types.is_integer_dtype(processed_df["event_timestamp"]):
        # Convert from unix timestamp in milliseconds to datetime
        processed_df["event_timestamp"] = pd.to_datetime(processed_df["event_timestamp"], unit='ms')
    else:
        # If it's already string in datetime format, convert to datetime
        processed_df["event_timestamp"] = pd.to_datetime(processed_df["event_timestamp"])
    
    logger.info(f"After timestamp conversion: {len(processed_df)} records")
    
    # Calculate activity_count (number of activities per user_session)
    activity_counts = processed_df.groupby("user_session").size().reset_index(name="activity_count")
    processed_df = processed_df.merge(activity_counts, on="user_session", how="left")
    logger.info(f"After adding activity_count: {len(processed_df)} records")
    
    # Split category_code into level1 and level2
    processed_df["category_code_level1"] = processed_df["category_code"].str.split(".").str[0]
    processed_df["category_code_level2"] = processed_df["category_code"].str.split(".").str[1]
    
    # Extract day of week (0-6, Monday=0)
    processed_df["event_weekday"] = processed_df["event_timestamp"].dt.dayofweek
    
    # Set is_purchased flag
    processed_df["is_purchased"] = np.where(processed_df["event_type"] == "purchase", 1, 0)
    
    # Fill null values
    processed_df["price"].fillna(0.0, inplace=True)
    processed_df["brand"].fillna("", inplace=True)
    processed_df["category_code_level1"].fillna("", inplace=True)
    processed_df["category_code_level2"].fillna("", inplace=True)
    
    # Filter out invalid records
    processed_df = processed_df[
        processed_df["user_id"].notna() & 
        processed_df["product_id"].notna() & 
        processed_df["price"].notna() & 
        (processed_df["price"] >= 0) & 
        processed_df["user_session"].notna()
    ]
    logger.info(f"After filtering: {len(processed_df)} records")
    
    # Select only the required columns
    feature_cols = [
        "event_timestamp", "user_id", "product_id", "user_session", "price",
        "brand", "category_code_level1", "category_code_level2", "event_weekday",
        "activity_count", "is_purchased"
    ]
    processed_df = processed_df[feature_cols]
    logger.info(f"Final processed data: {len(processed_df)} records")
    
    return processed_df

if __name__ == "__main__":
    main()