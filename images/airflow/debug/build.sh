#!/bin/bash
set -e

CONTAINER_RUNTIME=$1
AIRFLOW_VERSION=$2
PYTHON_VERSION=$3
echo "Using $CONTAINER_RUNTIME runtime in build.sh"

# Generate the Dockerfiles from the templates.
# shellcheck source=/dev/null
source "../../../.venv/bin/activate"
python3 ../generate-dockerfiles.py
deactivate

get_python_md5_checksum() {
  local python_version="$1"

  case "$python_version" in
    "3.10.8")
      echo "e92356b012ed4d0e09675131d39b1bde"
      ;;
    "3.11.6")
      echo "d0c5a1a31efe879723e51addf56dd206"
      ;;
    "3.11.7")
      echo "d96c7e134c35a8c46236f8a0e566b69c"
      ;;
    "3.11.9")
      echo "22ea467e7d915477152e99d5da856ddc"
      ;;
    *)
      echo "Unknown Python version: $python_version" >&2
      exit 9
      ;;
  esac
}


# Build the base image.
${CONTAINER_RUNTIME} build \
  --build-arg AIRFLOW_VERSION=$AIRFLOW_VERSION \
  --build-arg PYTHON_VERSION=$PYTHON_VERSION \
  --build-arg PYTHON_MD5_CHECKSUM=$(get_python_md5_checksum $PYTHON_VERSION) \
  -f ./Dockerfiles/Dockerfile.base -t amazon-mwaa-docker-images/airflow:debug-base ./

# Build the derivatives.
for dev in "True" "False"; do
    for build_type in "standard" "explorer" "explorer-privileged"; do
        dockerfile_name="Dockerfile"
        tag_name="debug"

        if [[ "$build_type" != "standard" ]]; then
            dockerfile_name="${dockerfile_name}-${build_type}"
            tag_name="${tag_name}-${build_type}"
        fi

        if [[ "$dev" == "True" ]]; then
            dockerfile_name="${dockerfile_name}-dev"
            tag_name="${tag_name}-dev"
        fi

        IMAGE_NAME="amazon-mwaa-docker-images/airflow:${tag_name}"
        ${CONTAINER_RUNTIME} build -f "./Dockerfiles/${dockerfile_name}" -t "${IMAGE_NAME}" ./
    done
done
