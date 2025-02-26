import os
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from dotenv import load_dotenv

# Tải biến môi trường từ file .env
load_dotenv()

# Lấy thông tin cấu hình từ biến môi trường
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID", "raw_events")
BUCKET_NAME = os.getenv("BUCKET_NAME")
LOCAL_DIR = os.getenv("LOCAL_DIR")  # Thư mục chứa file .json.gz

# Thiết lập credential
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# Khởi tạo client
storage_client = storage.Client()
bq_client = bigquery.Client()


# Bước 1: Upload file từ local lên GCS
def upload_files_to_gcs(local_dir, bucket_name):
    bucket = storage_client.get_bucket(bucket_name)
    for filename in os.listdir(local_dir):
        if filename.endswith(".json.gz"):
            local_path = os.path.join(local_dir, filename)
            blob = bucket.blob(f"event_dump/{filename}")
            blob.upload_from_filename(local_path)
            print(f"Uploaded {filename} to GCS")


# Bước 2: Định nghĩa schema
schema = [
    SchemaField("event_date", "STRING"),
    SchemaField("event_timestamp", "INT64"),
    SchemaField("event_name", "STRING"),
    SchemaField(
        "event_params",
        "RECORD",
        mode="REPEATED",
        fields=[
            SchemaField("key", "STRING"),
            SchemaField(
                "value",
                "RECORD",
                fields=[
                    SchemaField("int_value", "INT64", mode="NULLABLE"),
                    SchemaField("string_value", "STRING", mode="NULLABLE"),
                ],
            ),
        ],
    ),
    SchemaField("user_id", "STRING"),
    SchemaField(
        "geo",
        "RECORD",
        fields=[
            SchemaField("city", "STRING"),
            SchemaField("country", "STRING"),
            SchemaField("continent", "STRING"),
            SchemaField("region", "STRING"),
            SchemaField("sub_continent", "STRING"),
            SchemaField("metro", "STRING"),
        ],
    ),
]


# Bước 3: Tạo bảng BigQuery với schema và cấu hình phân vùng
def create_table():
    dataset_ref = bq_client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
    )
    table.clustering_fields = ["event_name", "event_date"]

    table = bq_client.create_table(table, exists_ok=True)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# Bước 4: Load dữ liệu từ GCS vào BigQuery
def load_data_to_bigquery():
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Ghi đè bảng nếu đã tồn tại
    )

    uri = f"gs://{BUCKET_NAME}/event_dump/*.json.gz"
    load_job = bq_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    load_job.result()  # Chờ job hoàn thành
    print(f"Loaded data into {TABLE_ID}. Total rows: {load_job.output_rows}")


# Thực thi quy trình
if __name__ == "__main__":
    # Upload file lên GCS
    upload_files_to_gcs(LOCAL_DIR, BUCKET_NAME)

    # Tạo bảng
    create_table()

    # Load dữ liệu
    load_data_to_bigquery()
