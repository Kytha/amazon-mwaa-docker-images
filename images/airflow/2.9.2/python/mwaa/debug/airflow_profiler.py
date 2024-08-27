import os
import sys
import subprocess
import argparse
from datetime import datetime, timedelta
import boto3
import time
import logging

AIRFLOW_PROFILER_SESSION_DURATION = 60
MWAA_AIRFLOW_PROFILER_S3_BUCKET = os.environ.get("MWAA__DEBUG__PROFILER_S3_BUCKET")
MWAA_ENABLE_AIRFLOW_PROFILER = os.environ.get("MWAA__DEBUG__PROFILER_ENABLED")
MWAA_AIRFLOW_COMPONENT = os.environ.get("MWAA_AIRFLOW_COMPONENT")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
CUSTOMER_ACCOUNT_ID = os.environ.get('CUSTOMER_ACCOUNT_ID', 'Unknown')
ENVIRONMENT_NAME = os.environ.get('AIRFLOW_ENV_NAME', 'Unknown')
PROFILER_SLEEP_DURATION_SECONDS = 60
PROFILER_OUTPUT_RETENTION_PERIOD_MINUTES = 15

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def get_airflow_process_command(airflow_component):
    if airflow_component == "scheduler":
        return "/usr/local/airflow/.local/bin/airflow scheduler"
    if airflow_component == "worker":
        return "-active- (celery worker)"
    if airflow_component == "webserver":
        return "airflow webserver"

def cleanup_old_files(directory_path, retention_period):
    current_time = datetime.now()
    initial_files = os.listdir(directory_path)
    for file_name in initial_files:
        try:
            timestamp_str = file_name.split('_')[1].split(".")[0]
            file_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d-%H-%M-%S')
            time_difference = current_time - file_timestamp
            if time_difference > timedelta(minutes=retention_period):
                file_path = os.path.join(directory_path, file_name)
                os.remove(file_path)
                print(f"Deleted old file: {file_name}")
        except Exception as e:
            logger.info(f"Unable to delete file {file_name}. Encounter exception: {e}")
    remaining_files = os.listdir(directory_path)
    logger.info(f"Deleted {len(initial_files) - len(remaining_files)} files. {len(remaining_files)} remain.")

def get_output_filename(pid):
    timestamp = get_timestamp()
    return f"{pid}_{timestamp}.json"

def get_output_filepath():
    return os.path.join("profiler", ENVIRONMENT_NAME, MWAA_AIRFLOW_COMPONENT)

def get_timestamp():
    now = datetime.now()
    return now.strftime("%Y-%m-%d-%H-%M-%S")

def execute_profiler_session(pid, duration, output_filepath):
    logger.info(pid)
    command = ["py-spy", "record", "--pid", pid, "-o", output_filepath, "--duration", str(duration), "--format", "speedscope", "--idle"]
    logger.info(f"Executing command: {command}")
    try:
        result = subprocess.run(command)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False

def get_airflow_process_pid(airflow_component):
    procs = subprocess.check_output(['ps', 'uaxw']).splitlines()
    logger.info("Found the following processes:")
    for proc in procs:
        logger.info(str(proc))
    airflow_command = get_airflow_process_command(airflow_component)
    airflow_proc = next(filter(lambda proc: airflow_command in str(proc), procs), None)
    if airflow_proc is None:
        return
    logger.info(f"Identified airflow process to be: {str(airflow_proc)}")
    return airflow_proc.split()[1].decode("utf-8")

def upload_file_to_s3(file_name, bucket_name, s3_key):
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_name, bucket_name, s3_key)
        logger.info(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        logger.info(f"Error uploading file to S3 bucket {bucket_name}: {e}")
        return False

if __name__ == "__main__":
    if not MWAA_ENABLE_AIRFLOW_PROFILER:
        logger.info("Airflow profiler is not enabled. Exiting...")
        sys.exit(1)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    while True:
        airflow_process_pid = get_airflow_process_pid(MWAA_AIRFLOW_COMPONENT)
        relative_file_path = get_output_filepath()
        absolute_file_path = os.path.join(AIRFLOW_HOME, relative_file_path)
        os.makedirs(absolute_file_path, exist_ok=True)
        if airflow_process_pid:
            file_name = get_output_filename(airflow_process_pid)

            logger.info(f"Executing profiler session for {AIRFLOW_PROFILER_SESSION_DURATION} seconds on process {airflow_process_pid}. Writing to file {os.path.join(absolute_file_path, file_name)}...")
            profiler_success = execute_profiler_session(airflow_process_pid, AIRFLOW_PROFILER_SESSION_DURATION, os.path.join(absolute_file_path, file_name))
            if profiler_success: 
                logger.info(f"Profiler session complete. Uploading results to s3 bucket {MWAA_AIRFLOW_PROFILER_S3_BUCKET} with key {os.path.join(relative_file_path, file_name)}")
                upload_file_to_s3(os.path.join(absolute_file_path, file_name), MWAA_AIRFLOW_PROFILER_S3_BUCKET, os.path.join(relative_file_path, file_name))
            else:
                logger.info("Profiler encounter error. Skipping s3 upload")
        else:
            logger.info(f"Unable to find {MWAA_AIRFLOW_COMPONENT} airflow process.")

        logger.info(f"Cleaning up profiler results in {absolute_file_path} older then: {PROFILER_OUTPUT_RETENTION_PERIOD_MINUTES} minutes")
        cleanup_old_files(absolute_file_path, PROFILER_OUTPUT_RETENTION_PERIOD_MINUTES)

        logger.info(f"Profiler sleeping for {PROFILER_SLEEP_DURATION_SECONDS} seconds...")
        time.sleep(PROFILER_SLEEP_DURATION_SECONDS)
