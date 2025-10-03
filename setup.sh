#!/bin/bash

# generate-env.sh
# สคริปต์สร้างไฟล์ .env สำหรับ Airflow + dbt

ENV_FILE=".env"

# ตรวจสอบว่ามีไฟล์ .env อยู่แล้วหรือไม่
if [ -f "$ENV_FILE" ]; then
    echo "⚠️  Found existing $ENV_FILE"
    read -p "Do you want to overwrite it? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "❌ Cancelled."
        exit 1
    fi
fi

# ดึง UID/GID ของ user ปัจจุบัน
USER_UID=$(id -u)
USER_GID=$(id -g)

cat > "$ENV_FILE" <<EOF
# --- Airflow Configuration ---
AIRFLOW_UID=${USER_UID}
AIRFLOW_GID=${USER_GID}
AIRFLOW_PROJ_DIR="."
# _PIP_ADDITIONAL_REQUIREMENTS="-r /opt/airflow/requirements.txt"

# ===== MinIO / S3 Config =====
S3_ENDPOINT_URL=http://minio:9000
S3_ACCESS_KEY_ID=admin
S3_SECRET_ACCESS_KEY=admin123456
S3_REGION=us-east-1
S3_ADDRESSING_STYLE=path

# bucket สำหรับผลลัพธ์ (warehouse = data lake)
S3_BUCKET=warehouse
# เก็บ raw csv
S3_RAW_PREFIX=assignment/raw
# เก็บ parquet/iceberg ที่แปลงแล้ว
S3_PREFIX=assignment/parquet
EOF

echo "✅ $ENV_FILE generated successfully!"
echo "   AIRFLOW_UID=$USER_UID"
echo "   AIRFLOW_GID=$USER_GID"
