import sys
import psycopg2
from pynetdicom import AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove as MoveModel
from pydicom.dataset import Dataset
import datetime
import calendar

# PostgreSQL connection details
DB_NAME = "pacs_inventory"
DB_USER = "postgres"  # Replace with your PostgreSQL username
DB_PASSWORD = "postgres"  # Replace with your PostgreSQL password
DB_HOST = "localhost"
DB_PORT = "5432"

# Local PACS details
LOCAL_PACS_HOST = "localhost"
LOCAL_PACS_PORT = "4242"
LOCAL_PACS_AET = "NDPACSMINI"

# Remote PACS details
REMOTE_PACS_HOST = "10.221.131.56"
REMOTE_PACS_PORT = "10358"
REMOTE_PACS_AET = "NNC-AI-FIND"

def main():
    if len(sys.argv) != 2:
        print("Usage: python cmove_script.py MM-YYYY")
        sys.exit(1)

    arg = sys.argv[1]
    try:
        month, year = map(int, arg.split('-'))
        if month < 1 or month > 12:
            raise ValueError("Invalid month")
    except ValueError:
        print("Invalid format. Use MM-YYYY, e.g., 08-2025")
        sys.exit(1)

    # Calculate first and last day of the month
    first_day = datetime.date(year, month, 1)
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = datetime.date(year, month, last_day_num)

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Query for study_instance_uids
        query = """
        SELECT study_instance_uid
        FROM study

        WHERE study_status = 0
        AND study_date >= %s
        AND study_date <= %s
        """
        cur.execute(query, (first_day, last_day))
        rows = cur.fetchall()
        uids = [row[0] for row in rows]

        if not uids:
            print("No studies found for the specified month and year with study_status=0.")
            cur.close()
            conn.close()
            return

        print(f"Found {len(uids)} studies to move.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        cur.close()
        conn.close()
        sys.exit(1)

    # Set up DICOM AE
    ae = AE(ae_title='CMOVE_SCU')  # Arbitrary AE title for the SCU
    ae.add_requested_context(MoveModel)

    # Associate with remote PACS
    assoc = ae.associate(REMOTE_PACS_HOST, int(REMOTE_PACS_PORT), ae_title=REMOTE_PACS_AET)

    if assoc.is_established:
        for uid in uids:
            ds = Dataset()
            ds.QueryRetrieveLevel = 'STUDY'
            ds.StudyInstanceUID = uid

            print(f"Initiating CMOVE for StudyInstanceUID: {uid}")

            try:
                responses = assoc.send_c_move(ds, LOCAL_PACS_AET, MoveModel)
                move_successful = False
                for (status, identifier) in responses:
                    if status and status.Status == 0x0000:  # Success status
                        move_successful = True
                        print(f"CMOVE successful for StudyInstanceUID: {uid}")
                    elif status:
                        print(f"CMOVE status: 0x{status.Status:04x}")
                    else:
                        print("Connection timed out, aborted or invalid response")

                # Update study_status to 1 if CMOVE was successful
                if move_successful:
                    try:
                        update_query = """
                        UPDATE study
                        SET study_status = 1
                        WHERE study_instance_uid = %s
                        """
                        cur.execute(update_query, (uid,))
                        conn.commit()
                        print(f"Updated study_status to 1 for StudyInstanceUID: {uid}")
                    except psycopg2.Error as e:
                        print(f"Failed to update study_status for {uid}: {e}")
                        conn.rollback()

            except Exception as e:
                print(f"Error during CMOVE for {uid}: {e}")

        assoc.release()
        cur.close()
        conn.close()
    else:
        print("Association rejected, aborted or never connected")
        cur.close()
        conn.close()
        sys.exit(1)

if __name__ == "__main__":
    main()