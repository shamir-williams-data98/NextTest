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
    os.makedirs(os.path.dirname(db_path),
                exist_ok=True)

    # Connect to database (it will create
    # if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table with appropriate
    # relationships
    ccursor.execute('''
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
