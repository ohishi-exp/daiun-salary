#!/bin/bash
set -euo pipefail

# .env 読み込み
set -a
source .env
set +a

IMAGE=asia-northeast1-docker.pkg.dev/cloudsql-sv/daiun-salary/daiun-salary:latest

echo "==> Building Docker image..."
docker build -t "$IMAGE" .

echo "==> Pushing to Artifact Registry..."
docker push "$IMAGE"

echo "==> Running migrations..."
sqlx migrate run --database-url "$DATABASE_URL"

echo "==> Deploying to Cloud Run..."
gcloud run deploy daiun-salary \
  --image "$IMAGE" \
  --region asia-northeast1 \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --memory 1Gi \
  --set-env-vars "DATABASE_URL=${DATABASE_URL}" \
  --set-env-vars "JWT_SECRET=${JWT_SECRET}" \
  --set-env-vars "GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID}" \
  --set-env-vars "GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET}" \
  --set-env-vars "R2_BUCKET=${R2_BUCKET}" \
  --set-env-vars "R2_ACCOUNT_ID=${R2_ACCOUNT_ID}" \
  --set-env-vars "R2_ACCESS_KEY=${R2_ACCESS_KEY}" \
  --set-env-vars "R2_SECRET_KEY=${R2_SECRET_KEY}" \
  --set-env-vars "GATEWAY_SECRET=${GATEWAY_SECRET}" \
  --set-env-vars "SCRAPER_URL=${SCRAPER_URL}" \
  --set-env-vars "CLOUD_TASKS_QUEUE=projects/cloudsql-sv/locations/asia-northeast1/queues/csv-split" \
  --set-env-vars "SELF_URL=https://daiun-salary-566bls5vfq-an.a.run.app" \
  --set-env-vars "SERVICE_ACCOUNT_EMAIL=747065218280-compute@developer.gserviceaccount.com"

echo "==> Done!"
gcloud run services describe daiun-salary --region=asia-northeast1 --format="value(status.url)"
