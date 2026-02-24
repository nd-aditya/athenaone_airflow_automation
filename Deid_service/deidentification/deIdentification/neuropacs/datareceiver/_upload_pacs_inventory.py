
# import os
# import csv
# import psycopg2
# from datetime import datetime
# from dateutil.relativedelta import relativedelta

# # Establish a connection to the PostgreSQL database
# def get_db_connection():
#     conn = psycopg2.connect(
#         dbname="pacs_inventory",  # Replace with your database name
#         user="postgres",     # Replace with your PostgreSQL username
#         password="postgres", # Replace with your PostgreSQL password
#         host="localhost",         # Replace with your host if necessary
#         port="5432"               # Replace with your port if necessary
#     )
#     return conn

# # Calculate patient's age based on DOB and StudyDate
# def calculate_patient_age(dob, study_date):
#     # Convert strings to datetime objects
#     dob = datetime.strptime(dob, "%Y-%m-%d")
#     study_date = datetime.strptime(study_date, "%Y-%m-%d")

#     # Calculate age
#     delta = relativedelta(study_date, dob)
    
#     if delta.years > 0:
#         return f"{delta.years:03}Y"  # Age in years
#     elif delta.months > 0:
#         return f"{delta.months:03}M"  # Age in months
#     else:
#         return f"{delta.days:03}D"  # Age in days

# # Insert data into PostgreSQL
# def insert_data_to_db(data):
#     conn = get_db_connection()
#     cursor = conn.cursor()

#     insert_query = """
#     INSERT INTO study (
#         study_date, study_time, accession_number, modalities_in_study, study_description, 
#         patient_name, patient_id, patient_birth_date, patient_sex, patient_age, 
#         study_instance_uid, number_of_study_related_series, number_of_study_related_instances
#     ) VALUES (
#         %(study_date)s, %(study_time)s, %(accession_number)s, %(modalities_in_study)s, %(study_description)s, 
#         %(patient_name)s, %(patient_id)s, %(patient_birth_date)s, %(patient_sex)s, %(patient_age)s, 
#         %(study_instance_uid)s, %(number_of_study_related_series)s, %(number_of_study_related_instances)s
#     );
#     """
    
#     cursor.executemany(insert_query, data)
#     conn.commit()
#     cursor.close()
#     conn.close()

# # Process CSV and insert data into the database
# def process_csv_and_insert(file_path):
#     # Open the CSV file
#     with open(file_path, mode='r', newline='') as csvfile:
#         csvreader = csv.DictReader(csvfile)
        
#         # Debugging: Print column headers to ensure we know the exact names
#         print(f"Processing file: {file_path}")
#         print(f"Column headers: {csvreader.fieldnames}")
        
#         # Prepare a list of dictionaries to store the rows
#         data_to_insert = []

#         # Iterate through the rows of the CSV
#         for row in csvreader:
#             # Remove extra spaces from column names (strip any leading/trailing spaces)
#             row = {key.strip(): value for key, value in row.items()}
            
#             # Check if PatientDOB and StudyDate are available before calculating PatientAge
#             if 'Patient Date of Birth' in row and row['Patient Date of Birth'] != '' and \
#                'Study Date' in row and row['Study Date'] != '':
#                 # Calculate PatientAge only if both Patient Date of Birth and Study Date are available
#                 patient_age = calculate_patient_age(row['Patient Date of Birth'], row['Study Date'])
#             else:
#                 patient_age = None  # If PatientDOB or StudyDate is missing, set PatientAge to None
            
#             # Convert empty date fields to None to avoid invalid date format error
#             study_date = row.get("Study Date", None)
#             study_time = row.get("Study Time", None)
#             patient_birth_date = row.get("Patient Date of Birth", None)
            
#             if not study_date or study_date == '':
#                 study_date = None
#             if not patient_birth_date or patient_birth_date == '':
#                 patient_birth_date = None
            
#             # Prepare data for insertion
#             data = {
#                 "study_date": study_date,
#                 "study_time": study_time,
#                 "accession_number": row.get("Accession Number", None),
#                 "modalities_in_study": row.get("Modalities in Study", None),
#                 "study_description": row.get("Study Description", None),
#                 "patient_name": row.get("Patient Name", None),
#                 "patient_id": row.get("Patient ID", None),
#                 "patient_birth_date": patient_birth_date,
#                 "patient_sex": row.get("Patient Sex", None),
#                 "patient_age": patient_age,
#                 "study_instance_uid": row.get("Study Instance UID", None),
#                 "number_of_study_related_series": row.get("Series Count", None),
#                 "number_of_study_related_instances": row.get("Instance Count", None)
#             }
            
#             # Append to the list for bulk insertion
#             data_to_insert.append(data)

#         # Insert the data into the database
#         insert_data_to_db(data_to_insert)

#     print(f"Data from {file_path} inserted successfully.")

# # Main function to process multiple CSV files
# def main():
#     # Folder path where your CSV files are located
#     folder_path = '/Users/ndainoran/Documents/MiniPACS/script/reports'  # Replace with your folder path

#     # Loop through all CSV files in the folder
#     for file_name in os.listdir(folder_path):
#         if file_name.endswith('.csv'):
#             file_path = os.path.join(folder_path, file_name)
#             process_csv_and_insert(file_path)

# if __name__ == "__main__":
#     main()
