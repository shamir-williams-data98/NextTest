#!/usr/bin/env python3
"""
Data extraction script for the technical
test.
Extracts data from CSV and saves it for
further processing.
"""
import os
import sqlite3
import pandas as pd
import logging
from datetime import datetime


# Database configuration
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                      'database', 'transactions.db')

# Output directory for extracted data
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                         'extracted_data')

def extract_data():
    """Extract data from the database and save it in Parquet format."""
    try:
        # Ensure the output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Connect to the database
        conn = sqlite3.connect(DB_PATH)
        
        # Query the data
        query = "SELECT * FROM raw_transactions"
        df = pd.read_sql_query(query, conn)
        
        # Close the connection
        conn.close()
        
        logger.info(f"Extracted {len(df)} rows from the database")
        
        # Define the output file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(OUTPUT_DIR, f"transactions_{timestamp}.parquet")
        
        # Save the data in Parquet format
        df.to_parquet(output_file, index=False)
        
        logger.info(f"Data saved to {output_file}")
        
        # Also save a CSV version for easier inspection
        csv_file = os.path.join(OUTPUT_DIR, f"transactions_{timestamp}.csv")
        df.to_csv(csv_file, index=False)
        
        logger.info(f"CSV version saved to {csv_file}")
        
        return output_file
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def main():
    """Main function to extract data from the database."""
    logger.info("Starting data extraction")
    
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found at {DB_PATH}")
        return
    
    # Extract the data
    output_file = extract_data()
    
    logger.info(f"Data extraction completed successfully. Output saved to {output_file}")

if __name__ == "__main__":
    main()


