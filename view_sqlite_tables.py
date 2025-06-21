# import sqlite3
# import pandas as pd
# import json

# # Connect to SQLite database
# conn = sqlite3.connect("data/tables.db")
# c = conn.cursor()

# # Query all tables
# c.execute("SELECT file_name, table_index, table_data FROM tables")
# rows = c.fetchall()

# # Print tables
# for file_name, table_index, table_data in rows:
#     print(f"\nFile: {file_name}, Table Index: {table_index}")
#     print("-" * 50)
#     # Convert JSON to DataFrame
#     df = pd.read_json(table_data, orient="columns")
#     print(df.to_string())
#     print("-" * 50)

# conn.close()

# # Saves tables from SQLite database to CSV files (1)
# import sqlite3
# import pandas as pd
# import json
# import os

# # Create output directory if it doesn't exist
# output_dir = "output_tables"
# os.makedirs(output_dir, exist_ok=True)

# # Connect to SQLite database
# conn = sqlite3.connect("data/tables.db")
# c = conn.cursor()

# # Query all tables
# c.execute("SELECT file_name, table_index, table_data FROM tables")
# rows = c.fetchall()

# # # Process and save tables as CSV
# for file_name, table_index, table_data in rows:
#     print(f"\nProcessing File: {file_name}, Table Index: {table_index}")
#     print("-" * 50)
#     # Convert JSON to DataFrame
#     df = pd.read_json(table_data, orient="columns")
#     print(df.to_string())
#     # Generate CSV filename
#     csv_filename = os.path.join(output_dir, f"{file_name}_table_{table_index}.csv")
#     # Save DataFrame to CSV
#     df.to_csv(csv_filename, index=False)
#     print(f"Saved to: {csv_filename}")
#     print("-" * 50)

# conn.close()


# ---------------------------------------------------------------------------------------------------

import sqlite3
import pandas as pd
import json
import os

# Create output directory if it doesn't exist
output_dir = "output_tables"
os.makedirs(output_dir, exist_ok=True)

# Connect to SQLite database
conn = sqlite3.connect("data/tables.db")
c = conn.cursor()

# Query the latest file based on the maximum id
c.execute("SELECT file_name FROM tables ORDER BY id DESC LIMIT 1")
latest_file = c.fetchone()

if latest_file:
    latest_file_name = latest_file[0]
    print(f"\nProcessing latest uploaded file: {latest_file_name}")
    print("-" * 50)

    # Query tables for the latest file
    c.execute("SELECT file_name, table_index, table_data FROM tables WHERE file_name = ?", (latest_file_name,))
    rows = c.fetchall()

    # Process and save tables as CSV
    for file_name, table_index, table_data in rows:
        print(f"Processing Table Index: {table_index}")
        print("-" * 50)
        # Convert JSON to DataFrame
        df = pd.read_json(table_data, orient="columns")
        print(df.to_string())
        # Generate CSV filename
        csv_filename = os.path.join(output_dir, f"{file_name}_table_{table_index}.csv")
        # Save DataFrame to CSV
        df.to_csv(csv_filename, index=False)
        print(f"Saved to: {csv_filename}")
        print("-" * 50)
else:
    print("No files found in the database.")

conn.close()


