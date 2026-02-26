#!/usr/bin/env sh
# =============================================================================
# Initialize MinIO Buckets and Folder Structure
# =============================================================================
set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET="${MINIO_BUCKET:-fraud-platform}"

echo "Configuring MinIO client..."
mc alias set minio "${MINIO_ENDPOINT}" "${MINIO_USER}" "${MINIO_PASS}" --insecure

# Wait for MinIO to be ready
echo "Waiting for MinIO..."
until mc ls minio > /dev/null 2>&1; do
    echo "  MinIO not ready, retrying..."
    sleep 3
done
echo "MinIO is ready."

# Create main bucket
echo "Creating bucket: ${BUCKET}"
mc mb --ignore-existing "minio/${BUCKET}"

# Set bucket policy to allow read/write
mc anonymous set none "minio/${BUCKET}"

# Create folder structure with placeholder files
LAYERS="bronze silver gold checkpoints"

for layer in ${LAYERS}; do
    echo "  Creating layer: ${layer}/"

    case ${layer} in
        bronze)
            TABLES="orders payments users devices geo_events refunds"
            ;;
        silver)
            TABLES="orders_enriched payments_enriched user_profiles fraud_features"
            ;;
        gold)
            TABLES="revenue_daily revenue_hourly fraud_summary user_fraud_scores product_metrics"
            ;;
        checkpoints)
            TABLES="bronze/orders bronze/payments bronze/users bronze/devices bronze/geo_events bronze/refunds"
            ;;
    esac

    for table in ${TABLES}; do
        path="minio/${BUCKET}/${layer}/${table}/.keep"
        echo "    Initializing: ${layer}/${table}/"
        printf "" | mc pipe "${path}"
    done
done

echo ""
echo "MinIO bucket structure created:"
mc tree "minio/${BUCKET}" --depth 2

echo ""

# Set lifecycle policy (delete checkpoints older than 7 days)
mc ilm rule add \
    --prefix "checkpoints/" \
    --expire-days 7 \
    "minio/${BUCKET}" || echo "Lifecycle policy: already set or not supported"

echo "MinIO initialization complete."
