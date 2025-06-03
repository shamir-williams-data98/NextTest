**Next Technical Test**

A simple data pipeline for processing transaction data.

**Overview**

This project implements a basic ETL (Extract, Transform, Load) pipeline for transaction data using Python and SQLite. The pipeline extracts raw transaction data, transforms it as needed, and stores it in a local SQLite database.

**Why SQLite?**

I went with SQLite for this project for a few key reasons:

1.
Serverless & Self contained - No need to run a separate server process. For a technical test like this, setting up a full database server would've been an overkill.

2. Zero Configuration - Just works out of the box. The database is a single file I can easily back up or share.

3. Portability - Everything's in one file that works across platforms, which made development much simpler.

4. Good Enough Speed - For the amount of data in this test, SQLite is plenty fast without the headaches of client-server databases.

5. Built into Python - The standard library has sqlite3, so I didn't need to install anything extra.

**Why Python?**

I chose Python for this pipeline because:

1. Great for Data Work - It handles data tasks really well, and I could add pandas or numpy later if needed.

2. Easy to Read - The code stays clean and understandable, which matters for a technical test where someone else needs to review it.

3. Works with SQLite Out of the Box - The sqlite3 module made database operations straightforward.

4. Quick to Build - I could get the pipeline up and running fast

**Database Schema**

The database consists of two tables: companies and raw_transactions with the following structure:







