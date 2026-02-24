# De-Identification System - Product Documentation

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Design](#architecture-design)
3. [Core Components](#core-components)
4. [Data Flow and Process Logic](#data-flow-and-process-logic)
5. [API Reference](#api-reference)
6. [Frontend Components](#frontend-components)
7. [Database Schema](#database-schema)
8. [De-Identification Rules](#de-identification-rules)
9. [PHI Detection Pipeline](#phi-detection-pipeline)
10. [Worker System](#worker-system)
11. [Deployment and Configuration](#deployment-and-configuration)
12. [Security and Compliance](#security-and-compliance)

---

## System Overview

The De-Identification System is a comprehensive healthcare data privacy solution designed to automatically identify, classify, and de-identify Protected Health Information (PHI) in Electronic Health Records (EHR) databases. The system combines AI-powered PHI detection with sophisticated de-identification rules to ensure HIPAA compliance while preserving data utility for research and analytics.

### Key Features
- **Automated PHI Detection**: Uses Large Language Models (LLM) to classify database columns
- **Multi-type PHI Classification**: Supports 7 different PHI types (Patient ID, Encounter ID, DOB, ZIP, Personal Info, Clinical Notes, Dates)
- **Structured and Unstructured Data Processing**: Handles both tabular data and clinical notes
- **Mapping Table System**: Maintains consistent patient/encounter ID mappings across datasets
- **Quality Control**: Automated validation and QC processes
- **Cloud Integration**: GCP upload and embedding generation
- **Web-based UI**: Modern React/Next.js interface for configuration and monitoring
- **Scalable Worker System**: Distributed task processing for large datasets

---

## Architecture Design

### High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                DE-IDENTIFICATION SYSTEM                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │   FRONTEND      │    │    BACKEND      │    │   WORKER        │            │
│  │   (Next.js)     │◄──►│   (Django)      │◄──►│   SYSTEM        │            │
│  │                 │    │                 │    │                 │            │
│  │ • Client Mgmt   │    │ • REST APIs     │    │ • Task Queue    │            │
│  │ • Config UI     │    │ • Auth System   │    │ • De-ID Engine  │            │
│  │ • Progress      │    │ • Data Models   │    │ • PHI Pipeline  │            │
│  │ • Monitoring    │    │ • Business Logic│    │ • QC Processes  │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
│           │                       │                       │                   │
│           │                       │                       │                   │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │   DATABASES     │    │   EXTERNAL      │    │   STORAGE       │            │
│  │                 │    │   SERVICES      │    │                 │            │
│  │ • Source DB     │    │ • OpenAI API    │    │ • GCP Storage   │            │
│  │ • Dest DB       │    │ • Keycloak      │    │ • File System   │            │
│  │ • Mapping DB    │    │ • Redis         │    │ • Logs          │            │
│  │ • PII DB        │    │                 │    │                 │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Component Interaction Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   USER      │    │  FRONTEND   │    │  BACKEND    │    │  WORKERS    │
│             │    │             │    │             │    │             │
└─────┬───────┘    └─────┬───────┘    └─────┬───────┘    └─────┬───────┘
      │                  │                  │                  │
      │ 1. Login         │                  │                  │
      ├─────────────────►│                  │                  │
      │                  │ 2. Auth Request  │                  │
      │                  ├─────────────────►│                  │
      │                  │                  │ 3. Create Tasks  │
      │                  │                  ├─────────────────►│
      │                  │                  │                  │
      │ 4. Configure     │                  │                  │
      ├─────────────────►│                  │                  │
      │                  │ 5. API Calls     │                  │
      │                  ├─────────────────►│                  │
      │                  │                  │ 6. Process Data  │
      │                  │                  │◄─────────────────┤
      │                  │ 7. Progress      │                  │
      │◄─────────────────┤◄─────────────────┤                  │
      │                  │                  │                  │
      │ 8. Monitor       │                  │                  │
      ├─────────────────►│                  │                  │
      │                  │ 9. Status Query  │                  │
      │                  ├─────────────────►│                  │
      │                  │                  │ 10. Get Results  │
      │                  │                  │◄─────────────────┤
      │                  │ 11. Display      │                  │
      │◄─────────────────┤                  │                  │
```

---

## Core Components

### Backend (Django)

#### 1. API Layer (`nd_api/`)
- **Client Management**: CRUD operations for healthcare clients
- **Dump Management**: Handle data dumps and processing configurations
- **Table Management**: Database table operations and metadata
- **De-identification Control**: Start/stop de-identification processes
- **Configuration Management**: PHI rules, mapping configs, QC settings
- **Authentication**: Keycloak integration for user management

#### 2. Core Processing (`core/`)
- **De-identification Engine**: Main processing logic for PHI removal
- **Rule System**: Configurable de-identification rules
- **Database Handlers**: Source and destination database connections
- **Mapping System**: Patient/encounter ID mapping management
- **PII Processing**: Personal information handling and masking

#### 3. PHI Analyzer (`phi_analyzer/`)
- **LLM Integration**: OpenAI API for intelligent PHI detection
- **Validation Pipeline**: Multi-stage PHI validation process
- **Classification Engine**: Column-by-column PHI classification
- **Result Management**: Store and manage PHI analysis results

#### 4. Worker System (`worker/`)
- **Task Queue**: Distributed task management
- **Worker Processes**: Background task execution
- **Chain Management**: Task dependency handling
- **Progress Tracking**: Real-time status updates

### Frontend (Next.js)

#### 1. Client Management
- **Client Dashboard**: Overview of all healthcare clients
- **Client Registration**: New client setup and configuration
- **Dump Management**: Data dump creation and management

#### 2. Configuration Interface
- **Table Configuration**: PHI marking and rule assignment
- **Mapping Setup**: Patient/encounter ID mapping configuration
- **QC Configuration**: Quality control settings
- **PII Configuration**: Personal information masking rules

#### 3. Monitoring and Control
- **Progress Tracking**: Real-time processing status
- **Task Management**: Start/stop/restart operations
- **Results Viewing**: De-identified data preview
- **Error Handling**: Failed task management and retry

---

## Data Flow and Process Logic

### 1. Client Registration and Setup

```
┌─────────────────────────────────────────────────────────────────┐
│                    CLIENT REGISTRATION FLOW                    │
└─────────────────────────────────────────────────────────────────┘

1. User creates new client
   ├── Client name, EMR type
   ├── Patient identifier columns
   ├── Default offset values
   └── Admin connection string

2. First dump registration
   ├── Dump name and date
   ├── Source database connection
   ├── Generate database statistics
   └── Load tables into portal database

3. Mapping table configuration
   ├── Patient mapping config
   ├── Encounter mapping config
   ├── Master table setup
   └── ND auto-increment ID assignment

4. Ready for de-identification
   ├── PHI configuration
   ├── QC configuration
   └── Process configuration
```

### 2. PHI Detection and Classification

```
┌─────────────────────────────────────────────────────────────────┐
│                    PHI DETECTION PIPELINE                      │
└─────────────────────────────────────────────────────────────────┘

1. Column Analysis
   ├── Extract column metadata
   ├── Sample data collection
   └── Table context analysis

2. LLM Classification
   ├── Send to OpenAI API
   ├── PHI type determination
   └── Confidence scoring

3. Validation Pipeline
   ├── Rule-based validation
   ├── Pattern matching
   └── Cross-reference checking

4. Result Storage
   ├── PHI classification results
   ├── Validation status
   └── User review flags
```

### 3. De-identification Process

```
┌─────────────────────────────────────────────────────────────────┐
│                  DE-IDENTIFICATION WORKFLOW                    │
└─────────────────────────────────────────────────────────────────┘

1. Data Preparation
   ├── Load source data
   ├── Apply ignore row rules
   └── Identify PHI columns

2. Mapping Resolution
   ├── Load patient mappings
   ├── Load encounter mappings
   ├── Load PII data
   └── Load insurance data

3. Rule Application
   ├── Patient ID replacement
   ├── Encounter ID replacement
   ├── Date offsetting
   ├── ZIP code masking
   ├── Personal info masking
   └── Notes de-identification

4. Quality Control
   ├── Format validation
   ├── Completeness check
   ├── Consistency verification
   └── Error reporting

5. Output Generation
   ├── Create destination tables
   ├── Insert de-identified data
   └── Generate processing reports
```

### 4. Unstructured Data Processing

```
┌─────────────────────────────────────────────────────────────────┐
│                UNSTRUCTURED DATA DE-IDENTIFICATION             │
└─────────────────────────────────────────────────────────────────┘

1. Text Analysis
   ├── Generic pattern detection
   ├── Date pattern identification
   └── PII value extraction

2. PII Masking
   ├── Name replacement
   ├── Address masking
   ├── Phone number masking
   └── Email masking

3. Date Processing
   ├── Date offsetting
   ├── Relative date calculation
   └── Date format preservation

4. XML Processing
   ├── XML structure analysis
   ├── Tag-based de-identification
   └── Schema validation

5. Final Output
   ├── De-identified text
   ├── Preserved formatting
   └── Audit trail
```

---

## API Reference

### Authentication Endpoints
- `POST /api/auth/login` - User authentication
- `POST /api/auth/logout` - User logout
- `GET /api/auth/user` - Get current user info

### Client Management
- `GET /api/clients/` - List all clients
- `POST /api/clients/` - Create new client
- `GET /api/clients/{id}/` - Get client details
- `PUT /api/clients/{id}/` - Update client
- `DELETE /api/clients/{id}/` - Delete client

### Dump Management
- `GET /api/client_dumps/{client_id}/` - List client dumps
- `POST /api/client_dumps/{client_id}/` - Create new dump
- `GET /api/dump_details/{client_id}/{dump_id}/` - Get dump details
- `POST /api/start_dump_processing/{client_id}/{dump_id}/` - Start processing

### Table Management
- `GET /api/get_tables/{client_id}/{dump_id}/` - List tables
- `GET /api/tables_details_for_ui/{table_id}/` - Get table details
- `GET /api/table_schema/{client_id}/{dump_id}/` - Get table schema
- `GET /api/view_table_data/{table_id}/` - View table data

### De-identification Control
- `POST /api/start_de_identification/{table_id}/` - Start table de-identification
- `POST /api/start_whole_identification/{client_id}/{dump_id}/` - Start bulk de-identification
- `POST /api/stop_de_identification/{table_id}/` - Stop de-identification
- `POST /api/deid/start/{client_id}/{dump_id}/` - Start bulk de-identification

### Configuration Management
- `GET /api/configuration/{client_id}/{dump_id}/` - Get configuration
- `POST /api/configuration/{client_id}/{dump_id}/` - Update configuration
- `GET /api/deid_rules/{client_id}/` - Get de-identification rules
- `POST /api/upload_config_from_csv/{client_id}/{dump_id}/` - Upload CSV config

### Quality Control
- `POST /api/qc/start/{client_id}/{dump_id}/` - Start QC process
- `GET /api/qc/result/{client_id}/{dump_id}/` - Get QC results

### Cloud Integration
- `POST /api/gcp/start/{client_id}/{dump_id}/` - Start GCP upload
- `GET /api/gcp/result/{client_id}/{dump_id}/` - Get GCP results
- `POST /api/embd/start/{client_id}/{dump_id}/` - Start embedding generation
- `GET /api/embd/result/{client_id}/{dump_id}/` - Get embedding results

---

## Frontend Components

### 1. Client Management (`/clients`)
- **ClientList**: Display all healthcare clients
- **ClientCard**: Individual client information card
- **ClientModal**: Create/edit client form
- **DumpModal**: Create new data dump
- **PacsModal**: PACS client management

### 2. Configuration Interface (`/clients/[clientId]/dumps/[dumpId]/configure`)
- **TableList**: List of database tables
- **ColumnConfig**: PHI marking interface
- **RuleAssignment**: De-identification rule assignment
- **MappingConfig**: Patient/encounter ID mapping
- **QcConfig**: Quality control settings

### 3. PHI Tables Management (`/clients/[clientId]/dumps/[dumpId]/phi_tables`)
- **TableListCard**: Table information and status
- **ProgressTracker**: Processing progress display
- **BulkActions**: Bulk operation controls
- **StatusIndicators**: Task status visualization

### 4. PACS Management (`/clients/[clientId]/pacs`)
- **PacsClientList**: PACS client overview
- **StudyBrowser**: DICOM study navigation
- **InstanceViewer**: DICOM instance management
- **DeidentificationControls**: PACS de-identification controls

### 5. Authentication (`/login`)
- **LoginForm**: User authentication form
- **PasswordReset**: Password reset functionality
- **SessionManagement**: User session handling

---

## Database Schema

### Core Models

#### Clients
```python
class Clients(models.Model):
    id = models.AutoField(primary_key=True)
    client_name = models.CharField(unique=True, max_length=200)
    emr_type = models.CharField(max_length=200)
    config = models.JSONField(default=dict)
    mapping_db_config = models.JSONField(default=dict)
    master_db_config = models.JSONField(default=dict)
    patient_identifier_columns = models.JSONField(default=list)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

#### ClientDataDump
```python
class ClientDataDump(models.Model):
    id = models.AutoField(primary_key=True)
    dump_name = models.CharField(unique=True, max_length=200)
    source_db_config = models.JSONField(default=dict)
    run_config = models.JSONField(default=dict)
    pii_config = models.JSONField(default=dict)
    secondary_config = models.JSONField(default=list)
    global_config = models.JSONField(default=list)
    qc_config = models.JSONField(default=dict)
    status = models.IntegerField(choices=DUMP_STATUS_CHOICES)
    client = models.ForeignKey(Clients, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

#### Table
```python
class Table(models.Model):
    id = models.AutoField(primary_key=True)
    table_name = models.CharField(max_length=255, db_index=True)
    rows_count = models.IntegerField(null=True)
    dump = models.ForeignKey(ClientDataDump, on_delete=models.CASCADE)
    table_details_for_ui = models.JSONField(default=dict)
    metadata = models.OneToOneField(TableMetadata, on_delete=models.CASCADE)
    deid = models.OneToOneField(TableDEIDStatus, on_delete=models.CASCADE)
    qc = models.OneToOneField(TableQCStatus, on_delete=models.CASCADE)
    gcp = models.OneToOneField(TableGCPStatus, on_delete=models.CASCADE)
    embd = models.OneToOneField(TableEmbeddingStatus, on_delete=models.CASCADE)
    is_phi_marking_done = models.BooleanField(default=False)
    is_phi_marking_locked = models.BooleanField(null=True)
    run_config = models.JSONField(default=dict)
    is_required = models.BooleanField(default=True)
```

### PHI Analysis Models

#### PHITableResult
```python
class PHITableResult(models.Model):
    id = models.AutoField(primary_key=True)
    session = models.ForeignKey(PHIAnalysisSession, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=200, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    total_columns = models.IntegerField(default=0)
    phi_columns = models.IntegerField(default=0)
    non_phi_columns = models.IntegerField(default=0)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    retry_count = models.IntegerField(default=0)
```

#### PHIColumnResult
```python
class PHIColumnResult(models.Model):
    id = models.AutoField(primary_key=True)
    table_result = models.ForeignKey(PHITableResult, on_delete=models.CASCADE)
    column_name = models.CharField(max_length=200, db_index=True)
    is_phi = models.CharField(max_length=10, choices=PHI_CHOICES)
    phi_rule = models.CharField(max_length=100, blank=True)
    pipeline_remark = models.TextField(blank=True)
    user_remarks = models.TextField(blank=True)
    is_manually_verified = models.BooleanField(default=False)
    verified_by = models.CharField(max_length=200, blank=True)
    verified_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### Worker System Models

#### Task
```python
class Task(models.Model):
    id = models.AutoField(primary_key=True)
    chain = models.ForeignKey(Chain, on_delete=models.CASCADE)
    type = models.CharField(max_length=100, db_index=True)
    arguments = models.JSONField(default=dict)
    remarks = models.JSONField(default=dict)
    status = models.IntegerField(db_index=True, default=ComputationStatus.NOT_STARTED)
    num_dependencies_total = models.IntegerField(default=0)
    num_dependencies_pending = models.IntegerField(db_index=True, default=0)
    timeout = models.IntegerField(default=60)
    started_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True, db_index=True)
    failure_count = models.IntegerField(default=0)
    soft_delete = models.BooleanField(default=False, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

---

## De-identification Rules

### Rule Types

#### 1. Patient ID Rules
- **PATIENT_ID**: Replace with mapped ND patient ID
- **REFERENCE_PID**: Reference patient ID mapping
- **REFER_PATIENT_ID**: Referenced patient ID handling

#### 2. Encounter ID Rules
- **ENCOUNTER_ID**: Replace with mapped ND encounter ID
- **APPOINTMENT_ID**: Appointment ID mapping

#### 3. Date Rules
- **DATE_OFFSET**: Apply date offsetting
- **STATIC_OFFSET**: Static date offset
- **32_OFFSET**: 32-bit date offset
- **PATIENT_DOB**: Date of birth offsetting

#### 4. Masking Rules
- **MASK**: General PII masking
- **ZIP_CODE**: ZIP code masking
- **NOTES**: Clinical notes de-identification
- **GENERIC_NOTES**: Generic notes processing

### Rule Implementation

```python
class BaseDeIdentificationRule:
    @classmethod
    def de_identify_value(cls, table_name: str, column_config: ColumnDetailsForUI, 
                         row: Any, patient_mapping_dict: dict, 
                         encounter_mapping_dict: dict, 
                         re_usable_bag: ReusableBag) -> tuple[Any, ReusableBag]:
        # Base implementation for all de-identification rules
        pass

class PatientIDDeIdntRule(BaseDeIdentificationRule):
    @classmethod
    def de_identify_value(cls, table_name: str, column_config: ColumnDetailsForUI, 
                         row: Any, patient_mapping_dict: dict, 
                         encounter_mapping_dict: dict, 
                         re_usable_bag: ReusableBag) -> tuple[Any, ReusableBag]:
        # Patient ID replacement logic
        original_value = row[column_config["column_name"]]
        if original_value in patient_mapping_dict:
            return patient_mapping_dict[original_value], re_usable_bag
        else:
            raise IgnoreRowException("Patient ID not found in mapping")
```

### Unstructured Data Processing

#### Generic Pattern De-identification
```python
class GenericPatternDeIdentification:
    def __init__(self, text: str, offset_value: int, date_parse_cache: dict):
        self.text = text
        self.offset_value = offset_value
        self.date_parse_cache = date_parse_cache
    
    def others(self):
        # Handle names, addresses, phone numbers, emails
        pass
    
    def date(self):
        # Handle date patterns and offsetting
        pass
```

#### PII Values Masking
```python
class PIIValuesMasking:
    def __init__(self, pii_config: dict, pii_data: dict, 
                 insurance_data: dict, text: str, date_parse_cache: dict):
        self.pii_config = pii_config
        self.pii_data = pii_data
        self.insurance_data = insurance_data
        self.text = text
        self.date_parse_cache = date_parse_cache
    
    def deidentify(self) -> str:
        # Apply PII masking based on configuration
        pass
```

---

## PHI Detection Pipeline

### LLM Agent Configuration

```python
class LLMAgent:
    def __init__(self, config: dict):
        self.model_name = config.get('model_name', 'gpt-4')
        self.api_key = config.get('api_key')
        self.temperature = config.get('temperature', 0.1)
        self.max_tokens = config.get('max_tokens', 1000)
    
    def classify_column(self, column_name: str, sample_values: List[Any], 
                       table_name: str) -> PHIClassificationResult:
        # Send to OpenAI API for PHI classification
        pass
```

### PHI Types Supported

1. **patientid**: Patient identifiers (MRN, Chart ID, etc.)
2. **encounterid**: Encounter/visit identifiers
3. **dob**: Date of birth fields
4. **zipcode**: ZIP/postal codes
5. **mask**: Personal information (names, addresses, SSN, etc.)
6. **notes**: Clinical notes and unstructured text
7. **date**: General date/time fields

### Validation Pipeline

```python
class PHIValidationToolsManager:
    def __init__(self, config: dict, db_manager: DatabaseManager, 
                 master_db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.master_db_manager = master_db_manager
        self.validators = self._initialize_validators()
    
    def validate_column(self, column_data: Dict[str, Any], 
                       llm_result: PHIClassificationResult) -> Tuple[bool, str, str]:
        # Multi-stage validation process
        pass
```

### Pipeline Optimization

The system uses a producer-consumer pattern for efficient processing:

```python
class PHIDeidentificationPipelineOptimized:
    def __init__(self, config_path: Optional[str] = None):
        self.config_manager = ConfigManager(config_path)
        self.db_manager = DatabaseManager(self.config_manager.get_db_config())
        self.llm_agent = LLMAgent(self.config)
        self.validation_manager = PHIValidationToolsManager(...)
        self.output_manager = OutputManager(self.config)
    
    def run_pipeline(self, tables: List[str]) -> Dict[str, Any]:
        # Parallel processing with producer-consumer threads
        pass
```

---

## Worker System

### Task Management

```python
class TaskWorker:
    def __init__(self, max_tasks_to_process=MAX_TASK_PER_WORKER, 
                 worker_poll_time=WORKER_POLL_TIME):
        self.worker_id = WORKER_ID
        self.max_tasks_to_process = max_tasks_to_process
        self.worker_poll_time = worker_poll_time
        self.machine_id = get_machine_id()
    
    def work(self):
        # Main worker loop
        while True:
            self.execute_ready_tasks()
            time.sleep(self.worker_poll_time)
    
    def execute_ready_tasks(self):
        # Execute available tasks
        pass
```

### Task Types

1. **De-identification Tasks**: Process individual tables
2. **PHI Analysis Tasks**: Run PHI detection pipeline
3. **QC Tasks**: Quality control validation
4. **GCP Upload Tasks**: Cloud storage operations
5. **Embedding Tasks**: Generate data embeddings
6. **Mapping Tasks**: Create/update mapping tables

### Chain Management

```python
class Chain(models.Model):
    id = models.AutoField(primary_key=True)
    reference_uuid = models.CharField(max_length=100, unique=True)
    adjacency_list = models.JSONField(default=dict)
    status = models.IntegerField(default=ChainStatus.NOT_STARTED)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### Worker Configuration

```python
# Worker settings
MAX_TASK_PER_WORKER = 5
WORKER_POLL_TIME = 1  # seconds
MACHINE_EXPIRY_OFFSET_TIMEOUT = 300  # seconds
SOFT_DELETE_AGE = 7  # days
```

---

## Deployment and Configuration

### Docker Configuration

#### Backend Dockerfile
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
```

#### Frontend Dockerfile
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build
CMD ["npm", "start"]
```

#### Docker Compose
```yaml
version: '3.8'
services:
  backend:
    build: ./deidentification
    ports:
      - "8000:8000"
    environment:
      - DJANGO_SETTINGS_MODULE=deIdentification.settings
    depends_on:
      - postgres
      - redis
  
  frontend:
    build: ./de-identification-ui
    ports:
      - "3000:3000"
    environment:
      - NEXT_PUBLIC_API_URL=http://localhost:8000
  
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_DB=deidentification
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  redis:
    image: redis:6-alpine
    ports:
      - "6379:6379"
```

### Environment Configuration

#### Backend Environment
```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/deidentification
MAPPING_DATABASE_URL=postgresql://user:password@localhost:5432/mapping

# OpenAI
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4

# Redis
REDIS_URL=redis://localhost:6379/0

# Keycloak
KEYCLOAK_SERVER_URL=http://localhost:8080
KEYCLOAK_REALM=deidentification
KEYCLOAK_CLIENT_ID=deidentification-ui
```

#### Frontend Environment
```bash
# API Configuration
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_IS_ONPREM=false

# Keycloak
NEXT_PUBLIC_KEYCLOAK_URL=http://localhost:8080
NEXT_PUBLIC_KEYCLOAK_REALM=deidentification
NEXT_PUBLIC_KEYCLOAK_CLIENT_ID=deidentification-ui
```

### Production Deployment

#### Kubernetes Configuration
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: deidentification-backend
spec:
  replicas: 3
  selector:
    matchLabels:
      app: deidentification-backend
  template:
    metadata:
      labels:
        app: deidentification-backend
    spec:
      containers:
      - name: backend
        image: deidentification:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: deidentification-secrets
              key: database-url
```

---

## Security and Compliance

### Data Protection

1. **Encryption at Rest**: All sensitive data encrypted in database
2. **Encryption in Transit**: HTTPS/TLS for all communications
3. **Access Control**: Role-based permissions system
4. **Audit Logging**: Comprehensive activity logging
5. **Data Retention**: Configurable data retention policies

### HIPAA Compliance

1. **PHI Identification**: Automated detection of all PHI types
2. **De-identification Standards**: Safe Harbor and Expert Determination methods
3. **Access Controls**: User authentication and authorization
4. **Audit Trails**: Complete processing history
5. **Data Minimization**: Only necessary data processing

### Security Measures

```python
# Authentication middleware
class AuthenticationMiddleware:
    def process_request(self, request):
        # Validate JWT token
        # Check user permissions
        # Log access attempts
        pass

# Data encryption
class DataEncryption:
    @staticmethod
    def encrypt_sensitive_data(data: str) -> str:
        # Encrypt PHI data
        pass
    
    @staticmethod
    def decrypt_sensitive_data(encrypted_data: str) -> str:
        # Decrypt PHI data
        pass
```

### Compliance Reporting

1. **Processing Reports**: Detailed de-identification reports
2. **Audit Logs**: User activity and system events
3. **Data Lineage**: Track data transformations
4. **Error Reports**: Failed processing attempts
5. **Performance Metrics**: System performance monitoring

---

## Conclusion

The De-Identification System provides a comprehensive, scalable solution for healthcare data privacy compliance. With its AI-powered PHI detection, sophisticated de-identification rules, and modern web interface, it enables healthcare organizations to safely process and share data while maintaining HIPAA compliance.

The system's modular architecture allows for easy customization and extension, while its worker-based processing ensures scalability for large datasets. The combination of automated PHI detection and manual verification capabilities provides both efficiency and accuracy in the de-identification process.

For technical support or additional information, please refer to the system logs, API documentation, or contact the development team.

---
