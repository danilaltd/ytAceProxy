#!/bin/bash

set -euo pipefail

trap 'echo "FAILED at line $LINENO"' ERR

start_time=$(date +%s)

BRANCH_NAME="${GITHUB_REF_NAME:-}"

if [ "$BRANCH_NAME" == "main" ]; then
    ENVIRONMENT="prod"
    export ACE_PORT=6878
    export MANAGER_PORT=8088
    export ACEXY_PORT=8090
    echo "Deploying to PRODUCTION environment"
else
    ENVIRONMENT="dev"
    export ACE_PORT=6879
    export MANAGER_PORT=8089
    export ACEXY_PORT=8091
    echo "Deploying to DEVELOPMENT environment"
fi


REPO=$(echo "$GITHUB_REPOSITORY" \
    | tr '[:upper:]' '[:lower:]')
DASHED_REPO=$(echo $REPO | tr '/' '-')
PROJECT_PATH="/var/lib/${DASHED_REPO}-${ENVIRONMENT}"
IMAGE_TAG="${ENVIRONMENT}-${GITHUB_RUN_NUMBER}"
export MANAGER_DB_PATH="${PROJECT_PATH}/manager-data/"
export ACESTREAM_CACHE_PATH="${PROJECT_PATH}/acestream-cache/"

mkdir -p "$PROJECT_PATH"
mkdir -p "$MANAGER_DB_PATH"
mkdir -p "$ACESTREAM_CACHE_PATH"


echo "Starting deployment for ${ENVIRONMENT} environment"
echo "Repository: ${REPO}"
echo "Image tag: ${IMAGE_TAG}"

docker login ghcr.io \
    -u "$GITHUB_ACTOR" \
    -p "$GITHUB_TOKEN"

export PROXY_IMAGE="ghcr.io/$REPO-proxy:$IMAGE_TAG"

export COMPOSE_PROJECT_NAME="${DASHED_REPO}-$ENVIRONMENT"

echo "Pulling images..."
docker compose pull --quiet

echo "Starting new containers..."
docker compose up -d --remove-orphans

# echo "Waiting for services to be healthy..."
# timeout 300 bash -c 'until docker compose ps --format json | grep -q manager.*healthy; do echo "Waiting for service..."; sleep 5; done'

end_time=$(date +%s)
echo "Deployment took $((end_time - start_time)) seconds"
echo "Deployment successful!"