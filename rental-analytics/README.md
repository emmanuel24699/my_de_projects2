# Rental Marketplace Analytics Pipeline

## Overview

This project implements a batch data processing pipeline for the **Rental Marketplace Analytics** platform, designed to extract, transform, and load (ETL) data from an Aurora MySQL database to Amazon Redshift for analytics. The pipeline supports key business metrics such as average listing price, occupancy rate, popular locations, top listings, bookings per user, booking duration, and repeat customer rate. It is orchestrated using **AWS Step Functions** and scheduled to run daily at 02:00 AM UTC via **Amazon EventBridge**.

The pipeline uses three AWS Glue jobs to process data through layers: Raw, Curated, and Presentation. Pre-existing Presentation Layer views already initialized based on incoming data automatically update when the Curated Layer is populated, delivering metrics to stakeholders.

## Architecture

<!-- ![Architecture Diagram](./images/architecture%20diagram.png) -->

<img src="./images/architecture diagram.png" alt="Architecture Diagram" width="1000" height="550"/>

The pipeline processes data in the following stages:

1. **Extract Raw Data**:

   - Script: `extract_raw.py`
   - Source: Aurora MySQL tables (`apartments`, `apartment_attributes`, `bookings`, `user_viewing`) via Glue Data Catalog (`rental_db_*`).
   - Output: S3 bucket (`s3://lab2-rental-analytics/data/raw/[table]/`) as Parquet with Snappy compression.
   - Features: Data quality check (`ColumnCount > 0`), incremental loads via **job bookmarks** (`id`, `booking_id`, `user_id+apartment_id+viewed_at`).

2. **Load to Redshift Raw Layer**:

   - Script: `s3_to_redshift_raw.py`
   - Source: S3 data via Glue Data Catalog (`rental_redshift_raw`).
   - Output: Redshift Raw Layer tables (`raw_data.stg_apartments`, `raw_data.stg_apartment_attributes`, `raw_data.stg_bookings`, `raw_data.stg_user_viewing`).
   - Features: Incremental merge using temporary tables.

3. **Transform to Curated Layer**:

   - Script: `redshift_raw_to_curated.py`
   - Source: Redshift Raw Layer (`raw_data.stg_*`).
   - Transformations:
     - `fact_bookings`: Filters `booking_status LIKE '%confirmed%'`
     - `dim_listings`: Joins `stg_apartments` and `stg_apartment_attributes`, filters non-null `price` and `cityname`.
     - `fact_user_views`: Casts `viewed_at` to `DATE`, renames `apartment_id` to `listing_id`.
     - `dim_users`: Aggregates user view counts.
   - Output: Redshift Curated Layer tables (`curated.fact_bookings`, `curated.dim_listings`, `curated.fact_user_views`, `curated.dim_users`).
   - Features: Incremental merge.

4. **Presentation Layer**:
   - Views: Created in Redshift `presentation` schema (e.g., `vw_avg_listing_price_weekly`, `vw_popular_locations_weekly`, `vw_top_listings_weekly`, `vw_bookings_per_user_weekly`, `vw_avg_booking_duration`, `vw_repeat_customer_rate`, `vw_occupancy_rate_monthly`).
   - Behavior: Automatically update when Curated Layer refreshes.
   - Metrics: Average listing price, occupancy rate, popular locations, top listings, bookings per user, booking duration, repeat customer rate.

### Data Flow

```
Aurora MySQL → S3 (Raw) → Redshift Raw Layer → Redshift Curated Layer → Redshift Presentation Layer (Views)
```

### AWS Services

- **AWS Glue**: Executes ETL scripts, manages Data Catalog (`rental_db`, `rental_redshift_raw`).
- **Amazon S3**: Stores raw data (`s3://lab2-rental-analytics/data/raw/`), scripts (`s3://lab2-rental-analytics/scripts/`), and Glue temporary files (`s3://lab2-rental-analytics/glue/temp`).
- **Amazon Redshift**: Hosts data warehouse (`rental_warehouse`) with Raw (`raw_data`), Curated (`curated`), and Presentation (`presentation`) schemas.
- **AWS Step Functions**: Orchestrates Glue jobs (`extract_raw`, `s3_to_redshift_raw`, `redshift_raw_to_curated`).
- **Amazon EventBridge**: Schedules daily execution at 02:00 AM UTC.
- **Amazon SNS**: Notifies on pipeline failures.

## Prerequisites

- **AWS Account**: With permissions to create Glue jobs, Step Functions, EventBridge rules, S3 buckets, Redshift clusters, and SNS topics.
- **Aurora MySQL**: Database `rental_db` with tables `apartments`, `apartment_attributes`, `bookings`, `user_viewing`.
- **Redshift Cluster**: Database `dev`, schemas `raw_data`, `curated`, `presentation`, and connection named “Redshift connection” in Glue.
- **S3 Bucket**: `lab2-rental-analytics` for data, scripts, and logs; `s3://lab2-rental-analytics/glue/temp` for Glue temporary files.
- **IAM Role**: `RentalPipelineGlueRole` with permissions for Glue, S3, Redshift, SNS, and CloudWatch Logs.
- **Glue Data Catalog**: Databases `rental_db` (Aurora MySQL) and `rental_redshift_raw` (S3 raw data).
- **SNS Topic**: For failure notifications.

## Setup Instructions

1. **Upload Scripts**:

   - Place `extract_raw.py`, `s3_to_redshift_raw.py`, `redshift_raw_to_curated.py` in `s3://lab2-rental-analytics/scripts/`.

   ```bash
   aws s3 cp extract_raw.py s3://lab2-rental-analytics/scripts/
   aws s3 cp s3_to_redshift_raw.py s3://lab2-rental-analytics/scripts/
   aws s3 cp redshift_raw_to_curated.py s3://lab2-rental-analytics/scripts/
   ```

2. **Create Glue Jobs**:

   - For each script, create a Glue job:
     - **Name**: `extract_raw`, `s3_to_redshift_raw`, `redshift_raw_to_curated`.
     - **Script Path**: `s3://lab2-rental-analytics/scripts/[script].py`.
     - **IAM Role**: `RentalPipelineGlueRole`.
     - **Glue Version**: 5.0.
     - **Temp Dir**:`s3://lab2-rental-analytics/glue/temp`.
     - **Bookmarking**: Enable for `extract_raw` (`--job-bookmark-option job-bookmark-enable`).

   ```bash
   aws glue create-job \
     --name extract_raw \
     --role RentalPipelineGlueRole \
     --command "Name=glueetl,ScriptLocation=s3://lab2-rental-analytics/scripts/extract_raw.py" \
     --default-arguments '{"--TempDir": "s3://lab2-rental-analytics/glue/temp", "--job-bookmark-option": "job-bookmark-enable"}' \
     --glue-version "5.0"
   ```

   Repeat for other jobs, omitting bookmarking for non-extraction jobs.

3. **Create Step Functions State Machine**:

   - Name: `RentalAnalyticsWorkflow`.
   - Definition: Use `rental_analytics_workflow.json` (see below).
   - IAM Role: Create with permissions for Glue and SNS.

   ```json
   {
     "Comment": "Orchestrates the Rental Marketplace Analytics ETL pipeline",
     "StartAt": "ExtractRaw",
     "States": {
       "ExtractRaw": {
         "Type": "Task",
         "Resource": "arn:aws:states:::glue:startJobRun.sync",
         "Parameters": {
           "JobName": "extract_raw"
         },
         "Retry": [
           {
             "ErrorEquals": ["States.TaskFailed"],
             "IntervalSeconds": 30,
             "MaxAttempts": 2,
             "BackoffRate": 2.0
           }
         ],
         "Catch": [
           {
             "ErrorEquals": ["States.ALL"],
             "Next": "NotifyFailure"
           }
         ],
         "Next": "LoadRaw"
       },
       "LoadRaw": {
         "Type": "Task",
         "Resource": "arn:aws:states:::glue:startJobRun.sync",
         "Parameters": {
           "JobName": "s3_to_redshift_raw"
         },
         "Retry": [
           {
             "ErrorEquals": ["States.TaskFailed"],
             "IntervalSeconds": 30,
             "MaxAttempts": 2,
             "BackoffRate": 2.0
           }
         ],
         "Catch": [
           {
             "ErrorEquals": ["States.ALL"],
             "Next": "NotifyFailure"
           }
         ],
         "Next": "CuratedLayer"
       },
       "CuratedLayer": {
         "Type": "Task",
         "Resource": "arn:aws:states:::glue:startJobRun.sync",
         "Parameters": {
           "JobName": "redshift_raw_to_curated"
         },
         "Retry": [
           {
             "ErrorEquals": ["States.TaskFailed"],
             "IntervalSeconds": 30,
             "MaxAttempts": 2,
             "BackoffRate": 2.0
           }
         ],
         "Catch": [
           {
             "ErrorEquals": ["States.ALL"],
             "Next": "NotifyFailure"
           }
         ],
         "Next": "Success"
       },
       "NotifyFailure": {
         "Type": "Task",
         "Resource": "arn:aws:states:::sns:publish",
         "Parameters": {
           "TopicArn": "<your-sns-topic-arn>",
           "Message": "Rental Analytics Pipeline failed. Check logs in s3://lab2-rental-analytics/logs/"
         },
         "Next": "Fail"
       },
       "Success": {
         "Type": "Succeed"
       },
       "Fail": {
         "Type": "Fail"
       }
     }
   }
   ```

   ```bash
   aws stepfunctions create-state-machine \
     --name RentalAnalyticsWorkflow \
     --definition file://rental_analytics_workflow.json \
     --role-arn arn:aws:iam::<account-id>:role/StepFunctionsRole
   ```

4. **Create EventBridge Rule**:

   - Name: `RunRentalAnalyticsDaily`.
   - Schedule: `cron(0 2 * * ? *)` (02:00 AM UTC).
   - Target: `RentalAnalyticsWorkflow`.
   - IAM Role: Create with `states:StartExecution` permission.

   ```bash
   aws events put-rule \
     --name RunRentalAnalyticsDaily \
     --schedule-expression "cron(0 2 * * ? *)"
   aws events put-targets \
     --rule RunRentalAnalyticsDaily \
     --targets "Id=1,Arn=arn:aws:states:<region>:<account-id>:stateMachine:RentalAnalyticsWorkflow,RoleArn=arn:aws:iam::<account-id>:role/EventBridgeRole"
   ```

5. **Configure IAM Role**:

   - `RentalPipelineGlueRole` with permissions for Glue, S3, Redshift, SNS, and CloudWatch Logs.

   - Create Step Functions role:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"],
           "Resource": "*"
         },
         {
           "Effect": "Allow",
           "Action": ["sns:Publish"],
           "Resource": "<your-sns-topic-arn>"
         }
       ]
     }
     ```
   - Create EventBridge role:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": ["states:StartExecution"],
           "Resource": "arn:aws:states:<region>:<account-id>:stateMachine:RentalAnalyticsWorkflow"
         }
       ]
     }
     ```

6. **Verify Glue Data Catalog**:
   - Run crawlers for `rental_db` (Aurora MySQL) and `rental_redshift_raw` (S3).
   ```bash
   aws glue start-crawler --name rental_db_crawler
   aws glue start-crawler --name rental_redshift_raw_crawler
   ```

## Usage

1. **Run Pipeline Manually**:

   - Start the Step Functions state machine:
     ```bash
     aws stepfunctions start-execution \
       --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:RentalAnalyticsWorkflow
     ```

2. **Verify Output**:

   - **S3 Raw Data**:
     ```bash
     aws s3 ls s3://lab2-rental-analytics/data/raw/
     ```
   - **Redshift Raw Layer**:
     ```sql
     SELECT COUNT(*) FROM raw_data.stg_bookings;
     SELECT COUNT(*) FROM raw_data.stg_apartments;
     ```
   - **Redshift Curated Layer**:
     ```sql
     SELECT COUNT(*) FROM curated.fact_bookings WHERE booking_status LIKE '%confirmed%';
     SELECT COUNT(*) FROM curated.dim_listings;
     ```
   - **Redshift Presentation Layer**:
     ```sql
     SELECT COUNT(*) FROM presentation.vw_avg_listing_price_weekly;
     SELECT COUNT(*) FROM presentation.vw_occupancy_rate_monthly;
     ```
   - **Logs**:
     ```bash
     aws s3 ls s3://lab2-rental-analytics/logs/
     ```

3. **Monitor Schedule**:
   - Check EventBridge rule `RunRentalAnalyticsDaily` for daily execution at 02:00 AM UTC.
   - Subscribe to SNS topic for failure notifications.

## Known Limitations

- **Catalog Dependency**: Pipeline relies on Glue Data Catalog (`rental_db`, `rental_redshift_raw`). Ensure crawlers are configured and updated.

## Troubleshooting

- **Pipeline Failure**:
  - Check Step Functions execution logs in AWS Console.
  - Review Glue job logs in `s3://lab2-rental-analytics/logs/`.
  - Ensure Redshift connection (“Redshift connection”) is valid.
- **Catalog Issues**:
  - Re-run Glue crawlers:
    ```bash
    aws glue start-crawler --name rental_db_crawler
    aws glue start-crawler --name rental_redshift_raw_crawler
    ```
- **Empty Views**:
  - Verify Curated Layer data:
    ```sql
    SELECT COUNT(*) FROM curated.fact_bookings;
    ```
  - Check view definitions for schema mismatches.

## Future Improvements

- Add CloudWatch alarms for job failures:
  ```bash
  aws cloudwatch put-metric-alarm \
    --alarm-name RentalPipelineFailure \
    --metric-name FailedExecutions \
    --namespace AWS/States \
    --dimensions Name=StateMachineArn,Value=arn:aws:states:<region>:<account-id>:stateMachine:RentalAnalyticsWorkflow \
    --comparison-operator GreaterThanThreshold \
    --threshold 0 \
    --period 300 \
    --evaluation-periods 1 \
    --alarm-actions <your-sns-topic-arn>
  ```
- Implement data validation checks post-transformation.
- Optimize Redshift distribution and sort keys for query performance.

##
