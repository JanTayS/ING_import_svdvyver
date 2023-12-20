ING Import Script
Overview
This Python script processes CSV files containing financial data, specifically looking for and extracting ING IDs, and then imports the processed data into an MS SQL Server database. It is designed to work with CSV files placed in a 'todo' folder, processes each file, and then moves them to a 'done' folder upon successful processing.

Prerequisites
Python 3.x
pandas
SQLAlchemy
A suitable database driver (pyodbc or pymssql)
An MS SQL Server database
Installation
Clone the Repository:


git clone [repository-url]
cd [repository-name]
Set up a Virtual Environment (Optional but Recommended):

python -m venv venv
source venv/bin/activate  # On Windows use 'venv\Scripts\activate'
Install Required Python Packages:
pip install -r requirements.txt


Configuration
Before running the script, ensure the database connection parameters in the script are correctly set:
server = 'your_server'
database = 'your_database'
username = 'your_username'
password = 'your_password'
Replace your_server, your_database, your_username, and your_password with your actual MS SQL Server details.

Usage
Place CSV Files:
Put your CSV files in the 'todo' folder located in the same directory as the script.

Run the Script:
python [script-name].py
The script will process each CSV file in the 'todo' folder, import the data into the specified SQL Server database, and then move the processed files to the 'done' folder.

Folder Structure
todo: Place your .csv files here for processing.
done: Processed files are moved here.
Notes
The script expects CSV files to have specific data formats, particularly looking for 'ING-id:' within the 'Mededeling begunstigde' column.
Ensure that the SQL Server is configured to allow connections and that the appropriate port is open (default is usually 1433).