# DATA108U_airflow

# Sparkify ETL with Apache Airflow

This project demonstrates the use of Apache Airflow to build an ETL (Extract, Transform, Load) pipeline for Sparkify, a music streaming startup. The ETL pipeline extracts data from JSON files stored on Amazon S3, transforms it, and loads it into a Redshift data warehouse. The main ETL DAG is defined in `dags/sparkify_etl.py`.

## DAG Overview

- **DAG Name:** sparkify_etl
- **Description:** Load and transform data in Redshift with Airflow
- **Schedule Interval:** @hourly

### Tasks

1. **start_execution:** An empty task to signal the start of the DAG execution.

2. **Stage_events:** Copies log data from Amazon S3 to Redshift's `staging_events` table. It uses the `StageToRedshiftOperator` operator.

3. **Stage_songs:** Copies song data from Amazon S3 to Redshift's `staging_songs` table. It also uses the `StageToRedshiftOperator` operator.

4. **Load_songs_table:** Loads data into the `songs` dimension table in Redshift using the `LoadDimensionOperator`. It truncates the table before loading.

5. **Load_songplays_table:** Loads data into the `songplays` fact table in Redshift using the `LoadFactOperator`.

6. **Load_users_table:** Loads data into the `users` dimension table in Redshift using the `LoadDimensionOperator`. It truncates the table before loading.

7. **Load_time_table:** Loads data into the `time` dimension table in Redshift using the `LoadDimensionOperator`. It truncates the table before loading.

8. **Load_artists_table:** Loads data into the `artists` dimension table in Redshift using the `LoadDimensionOperator`. It truncates the table before loading.

9. **Run_data_quality_checks:** Runs data quality checks on the loaded data to ensure its integrity and completeness. It uses the `DataQualityOperator` operator to perform various SQL checks.

10. **end_execution:** An empty task to signal the successful completion of the DAG execution.

### Dependencies

- `Stage_events` and `Stage_songs` tasks depend on `start_execution`.
- Loading tasks (`Load_songs_table`, `Load_songplays_table`, `Load_users_table`, `Load_time_table`, and `Load_artists_table`) depend on `Stage_events` and `Stage_songs`.
- Data quality checks task `Run_data_quality_checks` depends on all loading tasks.
- The DAG ends with the `end_execution` task.

## Configuration

- **Redshift Connection:** Configure the Redshift connection in Airflow with the connection ID `redshift`.
- **AWS Credentials:** Configure AWS credentials with the connection ID `aws_credentials`.
- **S3 Bucket:** Set the S3 bucket name for the source data in the DAG tasks.

## Usage

1. Configure the required connections (Redshift and AWS credentials) in the Airflow web UI.

2. Ensure the DAG execution interval and other settings in the `sparkify_etl` DAG are suitable for your needs.

3. Trigger the DAG manually or set up a schedule for it to run automatically.

4. Monitor DAG execution and check logs for any issues.

## Project Structure

- `dags/sparkify_etl.py`: The main DAG file that orchestrates the ETL process.
- `plugins/operators`: Custom operators for the ETL tasks.
- `plugins/helpers`: SQL queries and helper functions for the ETL process.

## Credits

This project is part of the Udacity Data Engineering Nanodegree program and is based on the provided project template and guidance.

Feel free to customize and extend this project to meet your specific ETL requirements.
