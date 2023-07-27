import pandas as pd
import pyodbc

# Replace these with your SQL Server connection details
server = 'DESKTOP-8LN5JLB'
database = 'InsuranceClaims'
driver = '{SQL Server}'

# Replace 'data.xlsx' with your actual Excel file path
excel_file_path = 'policies.xlsx'
sheet_name = 'policies'  # Replace with the sheet name containing the data

# Create a connection to the SQL Server database
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database}')

# Read data from Excel into a pandas DataFrame
df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

# Define the SQL query to insert data into the Policies table
sql_query = """
    INSERT INTO Policies (
        PolicyID, Country, VehicleType, VehicleUsage, Power, Weight, Colour,
        VehicleFirstRegistrationYear, VehicleAge, Mark, Model, EngineType,
        RenewalIndicator, Leasing, Deductible_general, Deductible_glass,
        Premium, SumInsured, PolicyPeriodInYears, Year
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Convert the DataFrame to a list of tuples (values to be inserted)
values = df.values.tolist()

# Create a cursor to execute the SQL query
cursor = conn.cursor()

try:
    # Execute the query for each row in the DataFrame
    cursor.executemany(sql_query, values)
    conn.commit()
    print("Data inserted successfully!")
except Exception as e:
    print("Error:", e)
    conn.rollback()
finally:
    cursor.close()
    conn.close()