# Game Analytics Pipeline

A data pipeline project for ingesting and analyzing gaming event data using Google Cloud Platform (GCP). This pipeline processes game events data to derive insights about user behavior, win rates, and skill usage patterns.

## 🎯 Features

- **Data Ingestion**

  - Automated upload of JSON files to Google Cloud Storage (GCS)
  - Configurable schema for BigQuery tables
  - Support for compressed files (\*.json.gz)

- **Data Processing**

  - Automatic table creation in BigQuery with optimized settings
  - Partitioning by day and clustering by event_name and event_date
  - Efficient data loading from GCS to BigQuery

- **Analytics Queries**
  - Win rate analysis for levels 1, 5, and 10
  - Average skill usage per game session for Brazilian users
  - User retention analysis across different game levels

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Google Cloud Platform account
- GCP project with enabled services:
  - Google Cloud Storage
  - BigQuery

### Installation

1. Clone the repository:

```bash
git clone https://github.com/nits302/game-analytics-pipeline.git
cd game-analytics-pipeline
```

2. Create and activate virtual environment:

```bash
python -m venv venv
source venv\Scripts\activate
```

3. Install required packages:

```bash
pip install -r requirements.txt
```

4. Set up environment variables:
   - Copy `.env.example` to `.env`
   - Update with your GCP credentials and configuration:
     ```env
     GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
     PROJECT_ID="your-project-id"
     DATASET_ID="your-dataset-id"
     BUCKET_NAME="your-bucket-name"
     LOCAL_DIR="path/to/local/data"
     ```

## 📊 Usage

### Data Ingestion

Run the ingestion script to upload data to GCS and BigQuery:

```bash
python ingestion_data.py
```
### Analytics Queries

The `SQL` folder contains pre-built queries for various analyses:

1. User Win Rate Analysis:

   ```sql
   -- SQL/user_win_rate.sql
   -- Analyzes win rates at levels 1, 5, and 10

   ```

2. Brazilian Users Skill Usage:
   sql

   ```-- SQL/brazil_user_avg_skill_usage.sql
   -- Calculates average skill usage per game session

   ```

3. User Retention Analysis:
   ```sql
   -- SQL/user_retention_by_level.sql
   -- Tracks user retention across different levels
   ```

## 🛠️ Technical Details

### Data Schema

The BigQuery table schema includes:

- event_date (STRING)
- event_timestamp (INT64)
- event_name (STRING)
- event_params (REPEATED RECORD)
- user_id (STRING)
- geo (RECORD)
  - city
  - country
  - continent
  - region
  - sub_continent
  - metro

### Optimizations

- Table partitioning by day
- Clustering by event_name and event_date
- Compressed file support
- Efficient batch loading
