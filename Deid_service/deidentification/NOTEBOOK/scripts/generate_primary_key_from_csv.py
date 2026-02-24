import pandas as pd

# Read the input CSV
input_file = "/Users/rohitchouhan/Documents/Code/Dump/athenaonedataview.csv"
output = "/Users/rohitchouhan/Documents/Code/Dump/athenaone_primarykey.csv"
df = pd.read_csv(input_file)

# Filter out CONTEXTID
df = df[df["COLUMNNAME"] != "CONTEXTID"]

# Keep only columns marked as Primary Key
df_pk = df[df["PRIMARYKEY"].str.strip().str.lower() == "primary key"]

# Group by TABLE NAME and combine PK columns (comma-separated for composite keys)
result = df_pk.groupby("TABLE NAME")["COLUMNNAME"].apply(lambda cols: ",".join(cols)).reset_index()

# Rename column
result.rename(columns={"COLUMNNAME": "PRIMARY_KEY_COLUMNS"}, inplace=True)

# Save to CSV
result.to_csv(output, index=False)

print("Processed CSV saved as 'tables_primary_keys_processed.csv'")
