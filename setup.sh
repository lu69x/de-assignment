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

# --- DBT Configuration ---
DBT_PROJ_DIR="./dbt"
EOF

echo "✅ $ENV_FILE generated successfully!"
echo "   AIRFLOW_UID=$USER_UID"
echo "   AIRFLOW_GID=$USER_GID"
