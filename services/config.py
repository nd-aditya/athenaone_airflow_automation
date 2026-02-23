# ===========================================
# SIMPLE CONFIG FILE - CHANGE VALUES HERE
# ===========================================

# Snowflake Connection
SNOWFLAKE_USER = 'sgarai'
SNOWFLAKE_PASSWORD = 'NDiscovery%401234'
SNOWFLAKE_ACCOUNT = 'CM97887-READER_DATAPOND_DLSE_PROD_NEURO_DISCOVERY_AI'
SNOWFLAKE_DATABASE = 'ATHENAHEALTH'
SNOWFLAKE_WAREHOUSE = 'AH_WAREHOUSE'
SNOWFLAKE_INSECURE_MODE = True  # Bypass SSL certificate validation

# MySQL Connection
MYSQL_USER = 'ndadmin'
MYSQL_PASSWORD = 'ndADMIN%402025'
MYSQL_HOST = 'localhost'

# Schemas
INCREMENTAL_SCHEMA = 'dump_daily'  # Where we extract data
HISTORICAL_SCHEMA = 'athenaone'  # Where we merge data

# Extraction Settings
CONTEXT_IDS = (1, 1367)
BATCH_SIZE = 10000
MAX_THREADS = 10
TEST_TABLES = None  # Testing with single table

# Email for notifications
EMAIL_RECIPIENTS = ['aalind@neurodiscovery.ai']

# Date settings (None = yesterday, or specify 'YYYY-MM-DD')
EXTRACTION_DATE = None

# Date range settings (None = daily extraction, or specify date range)
FROM_DATE = '2025-05-31'  # Start date for extraction (YYYY-MM-DD format)
TO_DATE = '2025-06-01'    # End date for extraction (YYYY-MM-DD format)

# ===========================================
# GOOGLE CHAT WEBHOOK NOTIFICATIONS
# ===========================================
# To get webhook URL:
# 1. Go to Google Chat space
# 2. Click space name → Manage webhooks
# 3. Add webhook and copy the URL
# Example: 'https://chat.googleapis.com/v1/spaces/XXXXX/messages?key=XXXXX&token=XXXXX'

GOOGLE_CHAT_WEBHOOK = 'https://chat.googleapis.com/v1/spaces/AAQAQEnoR7w/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=Jw4x8jS2oegUKr-9kbi0sH0sUlrCG0RBZJ95A6y2QnE'  # Add your webhook URL here

# Notification Settings
ENABLE_CHAT_NOTIFICATIONS = False  # Set to True to enable notifications
NOTIFY_ON_START = True             # Notify when pipeline starts
NOTIFY_ON_STEP = True             # Notify after each step (can be noisy)
NOTIFY_ON_SUCCESS = True           # Notify when pipeline completes successfully
NOTIFY_ON_FAILURE = True           # Notify when pipeline fails
