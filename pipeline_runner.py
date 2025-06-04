#!/usr/bin/env python3
"""
Main Runner Script for Section 1
This script will orchestrate the entire data processing pipeline.
"""

import os
import logging
import subprocess
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script paths
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_SETUP_SCRIPT = os.path.join(SCRIPTS_DIR, 'database_setup.py')
DATA_EXTRACTION_SCRIPT = os.path.join(SCRIPTS_DIR, 'data_extraction.py')
DATA_TRANSFORMATION_SCRIPT = os.path.join(SCRIPTS_DIR, 'data_transformation.py')

# CSV and database paths
CSV_PATH = os.path.join(os.path.dirname(SCRIPTS_DIR), 'data', 'data_prueba_técnica.csv')
DB_PATH = os.path.join(os.path.dirname(SCRIPTS_DIR), 'database', 'transactions.db')

def run_script(script_path):
    """Run a Python script and return its exit code."""
    try:
        logger.info(f"Running script: {script_path}")
        result = subprocess.run(['python3', script_path], check=True)
        logger.info(f"Script completed successfully: {script_path}")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running script {script_path}: {e}")
        return e.returncode

def import_csv_data():
    """Import CSV data into the database using the function from database_setup.py"""
    try:
        logger.info(f"Importing CSV data from {CSV_PATH}")
        
        # Import the function from database_setup.py
        import sys
        sys.path.append(SCRIPTS_DIR)
        from database_setup import import_csv_to_database
        
        # Make sure the data directory exists
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        
        # Check if CSV file exists at the expected location
        if not os.path.exists(CSV_PATH):
            logger.warning(f"CSV file not found at {CSV_PATH}")
            logger.info("Looking for CSV file in current directory...")
            
            # Try to find the CSV file in the current directory
            for file in os.listdir('.'):
                if file.endswith('.csv'):
                    logger.info(f"Found CSV file: {file}")
                    import_csv_to_database(file, DB_PATH)
                    return True
            
            logger.error("No CSV file found. Please place the CSV file in the data directory.")
            return False
        
        # Import the CSV data
        success = import_csv_to_database(CSV_PATH, DB_PATH)
        if success:
            logger.info("CSV data imported successfully")
            return True
        else:
            logger.error("Failed to import CSV data")
            return False
    except Exception as e:
        logger.error(f"Error importing CSV data: {e}")
        return False

def main():
    """Main function to run the entire data processing pipeline."""
    logger.info("Starting data processing pipeline")
    
    # Step 1: Set up the database
    if run_script(DATABASE_SETUP_SCRIPT) != 0:
        logger.error("Database setup failed. Aborting pipeline.")
        return
    
    # Step 2: Import CSV data into the database
    if not import_csv_data():
        logger.error("CSV import failed. Aborting pipeline.")
        return
    
    # Step 3: Extract data from the database
    if run_script(DATA_EXTRACTION_SCRIPT) != 0:
        logger.error("Data extraction failed. Aborting pipeline.")
        return
    
    # Step 4: Transform data and load into structured database
    if run_script(DATA_TRANSFORMATION_SCRIPT) != 0:
        logger.error("Data transformation failed. Aborting pipeline.")
        return
    
    logger.info("Data processing pipeline completed successfully")

if __name__ == "__main__":
    main()

