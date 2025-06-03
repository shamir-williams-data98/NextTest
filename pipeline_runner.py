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

def main():
    """Main function to run the entire data processing pipeline."""
    logger.info("Starting data processing pipeline")
    
    # Step 1: Set up the database and load raw data
    if run_script(DATABASE_SETUP_SCRIPT) != 0:
        logger.error("Database setup failed. Aborting pipeline.")
        return
    
    # Step 2: Extract data from the database
    if run_script(DATA_EXTRACTION_SCRIPT) != 0:
        logger.error("Data extraction failed. Aborting pipeline.")
        return
    
    # Step 3: Transform data and load into structured database
    if run_script(DATA_TRANSFORMATION_SCRIPT) != 0:
        logger.error("Data transformation failed. Aborting pipeline.")
        return
    
    logger.info("Data processing pipeline completed successfully")

if __name__ == "__main__":
    main()
