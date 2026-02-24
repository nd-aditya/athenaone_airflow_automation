import pandas as pd
from sqlalchemy import create_engine

CONNECTION_STRING = "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema"
# CONNECTION_STRING = "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema_large"

NUM_ROWS = 4

def make_table1(n=NUM_ROWS):
    return pd.DataFrame({
        "patient_id": range(1000, 1000 + n),
        "chartid": range(2000, 2000 + n),
        "notes": [f"notes {i}" for i in range(n)],
    })

def make_table2(n=NUM_ROWS):
    return pd.DataFrame({
        "chartid": range(2000, 2000 + n),
        "profileid": range(3000, 3000 + n),
        "dept": [f"dept {i%4}" for i in range(n)],
    })

def make_table3(n=NUM_ROWS):
    return pd.DataFrame({
        "patient_id": range(1000, 1000 + n),
        "pid": range(4000, 4000 + n),
        "visit": range(n),
    })

def make_table4(n=NUM_ROWS):
    return pd.DataFrame({
        "pid": range(4000, 4000 + n),
        "profileid": range(3000, 3000 + n),
        "status": ['A' if i % 2 == 0 else 'I' for i in range(n)],
    })

def main():
    engine = create_engine(CONNECTION_STRING)

    df1 = make_table1()
    df2 = make_table2()
    df3 = make_table3()
    df4 = make_table4()

    df1.to_sql("table1", engine, if_exists='replace', index=False)
    df2.to_sql("table2", engine, if_exists='replace', index=False)
    df3.to_sql("table3", engine, if_exists='replace', index=False)
    df4.to_sql("table4", engine, if_exists='replace', index=False)

    print("Dummy integer-typed patient identifier tables are created and populated!")

if __name__ == "__main__":
    main()
