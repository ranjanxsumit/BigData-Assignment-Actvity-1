import os
import sys

# Add the project's root directory to sys.path so that the "src" package can be found.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import sqlite3
import pytest
from src.main.lab import process_data

# Define the path to the SQLite database as used in your lab script.
DB_PATH = os.path.join("src", "data", "transactions.db")

def remove_database():
    """Remove the database file if it exists."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def test_db_file_created():
    """Test that process_data() creates the SQLite database file."""
    remove_database()  # Ensure a clean slate for the test.
    process_data()
    assert os.path.exists(DB_PATH), "Database file was not created."

def test_transactions_table_not_empty():
    """
    Test that the transactions table contains data.
    This confirms that the CSV was read and inserted into the table.
    """
    # Run the data processing to insert data.
    process_data()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Count the rows in the transactions table.
    cursor.execute("SELECT COUNT(*) FROM transactions")
    row_count = cursor.fetchone()[0]
    conn.close()
    
    assert row_count > 0, "The transactions table is empty."

def test_stdout_contains_expected_strings(capsys):
    """
    Test that running process_data() prints expected output strings.
    The lab script prints descriptive text for each SQL query output.
    """
    process_data()
    captured = capsys.readouterr().out

    # Check for a few key strings that should appear in the output.
    assert "Top 5 Best-Selling Products:" in captured
    assert "Monthly Revenue Trend:" in captured
    assert "Payment Method Popularity:" in captured
    assert "✅ Data Processing & SQL Analysis Completed Successfully!" in captured
