import pandas as pd

file_path = "/Users/michaelchia/Documents/Projects/Python/Stocks dbt/athlete_data_1000.csv"
output_file = "/Users/michaelchia/Documents/Projects/Python/Stocks dbt/test.xlsx"

# Read CSV — keep all columns as strings
df = pd.read_csv(file_path, dtype=str, keep_default_na=False, encoding="ISO-8859-1")

# Ensure the timestamp column is treated as text
# Replace 'timestamp' with your actual column name
df['Timestamp'] = df['Timestamp'].astype(str)

# Export to Excel, forcing text format in Excel
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df.to_excel(writer, index=False)

    # Force Excel to treat the timestamp column as text
    worksheet = writer.sheets['Sheet1']
    for cell in worksheet['A']:  # Replace 'A' with the column letter of your timestamp
        cell.number_format = '@'  # '@' in Excel means text format