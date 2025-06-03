#!/usr/bin/env python3
"""
Data transformation script for the technical test.
Transforms extracted CSV data and prepares it for loading into the
database.
"""
import os
import sqlite3
import pandas as pd
import logging
from datetime import datetime

# Set up logging

# Database configuration
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                      'database', 'structured_transactions.db')

# Input directory for extracted data
INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                        'extracted_data')

def create_structured_database():
    """Create a structured database with proper relationships."""
    try:
        # Ensure the database directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        # Connect to the database (creates it if it doesn't exist)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Create the companies table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            company_id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
        ''')
        
        # Create the charges table with a foreign key reference to companies
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS charges (
            id TEXT PRIMARY KEY,
            company_id TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            created_at DATE NOT NULL,
            paid_at DATE,
            FOREIGN KEY (company_id) REFERENCES companies (company_id)
        )
        ''')
        
        conn.commit()
        logger.info(f"Structured database created successfully at {DB_PATH}")
        return conn
    except Exception as e:
        logger.error(f"Error creating structured database: {e}")
        raise

def get_latest_extracted_file():
    """Get the latest extracted Parquet file."""
    try:
        parquet_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.parquet')]
        if not parquet_files:
            logger.error(f"No Parquet files found in {INPUT_DIR}")
            return None
        
        # Sort by filename (which includes timestamp)
        latest_file = sorted(parquet_files)[-1]
        return os.path.join(INPUT_DIR, latest_file)
    except Exception as e:
        logger.error(f"Error getting latest extracted file: {e}")
        raise

def transform_and_load_data(conn, input_file):
    """Transform the data and load it into the structured database."""
    try:
        # Read the Parquet file
        df = pd.read_parquet(input_file)
        logger.info(f"Loaded Parquet file with {len(df)} rows")
        
        # Transform the data
        
        # 1. Handle date columns
        df['created_at'] = pd.to_datetime(df['created_at']).dt.date
        df['paid_at'] = pd.to_datetime(df['paid_at']).dt.date
        
        # 2. Extract unique companies
        companies_df = df[['company_id', 'name']].drop_duplicates()
        logger.info(f"Extracted {len(companies_df)} unique companies")
        
        # 3. Prepare charges data
        charges_df = df[['id', 'company_id', 'amount', 'status', 'created_at', 'paid_at']]
        
        # Insert companies data
        companies_df.to_sql('companies', conn, if_exists='replace', index=False)
        
        # Insert charges data
        charges_df.to_sql('charges', conn, if_exists='replace', index=False)
        
        # Verify the data was loaded
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM companies")
        companies_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM charges")
        charges_count = cursor.fetchone()[0]
        
        logger.info(f"Inserted {companies_count} rows into companies table")
        logger.info(f"Inserted {charges_count} rows into charges table")
        
        return companies_count, charges_count
    except Exception as e:
        logger.error(f"Error transforming and loading data: {e}")
        raise

def create_daily_transactions_view(conn):
    """Create a view showing total transaction amount per day by company."""
    try:
        cursor = conn.cursor()
        
        # Create the view
        cursor.execute('''
        CREATE VIEW IF NOT EXISTS daily_transactions AS
        SELECT 
            c.name AS company_name,
            ch.created_at AS transaction_date,
            SUM(ch.amount) AS total_amount
        FROM 
            charges ch
        JOIN 
            companies c ON ch.company_id = c.company_id
        GROUP BY 
            c.name, ch.created_at
        ORDER BY 
            ch.created_at, c.name
        ''')
        
        conn.commit()
        logger.info("Created daily_transactions view successfully")
        
        # Test the view
        cursor.execute("SELECT * FROM daily_transactions LIMIT 5")
        sample = cursor.fetchall()
        logger.info(f"Sample data from view: {sample}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating view: {e}")
        raise

def main():
    """Main function to transform data and load it into a structured database."""
    logger.info("Starting data transformation and loading")
    
    # Get the latest extracted file
    input_file = get_latest_extracted_file()
    if not input_file:
        return
    
    # Create the structured database
    conn = create_structured_database()
    
    # Transform and load the data
    companies_count, charges_count = transform_and_load_data(conn, input_file)
    
    # Create the daily transactions view
    create_daily_transactions_view(conn)
    
    logger.info(f"Data transformation and loading completed successfully.")
    logger.info(f"Loaded {companies_count} companies and {charges_count} charges.")
    
    conn.close()

if __name__ == "__main__":
    main()
