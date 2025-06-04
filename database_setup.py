#!/usr/bin/env python3
"""
Database setup script for the
technical test.
Creates SQLite database with
appropriate table and relationships.
SQLite is Serverless and easy to use
with built in Python module
"""

import sqlite3
import os
import pandas as pd

def create_database(db_path):
    """
    Create a new SQLite database with
    the required schema.
    
    Args:
        db_path (str): Path to the
        SQLite database file
        
    Returns:
        bool: True if database was
        created successfully
    """
    # Create directory if it doesn't
    # exist as yet
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to database (it will create
    # if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table with appropriate
    # relationships
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_transactions (
            id TEXT PRIMARY KEY,
            name TEXT,
            company_id TEXT,
            amount REAL,
            status TEXT,
            created_at DATE,
            paid_at DATE
        )
    ''')
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    return True

def import_csv_to_database(csv_path, db_path):
    """
    Import CSV data into the SQLite database.
    
    Args:
        csv_path (str): Path to the CSV file
        db_path (str): Path to the SQLite database
        
    Returns:
        bool: True if import was successful
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Insert data into raw_transactions table
        df.to_sql('raw_transactions', conn, if_exists='replace', index=False)
        
        # Close connection
        conn.close()
        
        print(f"Successfully imported {len(df)} records from CSV to database")
        return True
    except Exception as e:
        print(f"Error importing CSV: {e}")
        return False

def main():
    """Main function to set up the database."""
    # Define database path
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'database', 'transactions.db')
    
    # Create the database
    create_database(db_path)
    
    return True

if __name__ == "__main__":
    main()

