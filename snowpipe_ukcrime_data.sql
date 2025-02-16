-- Configure Azure blob storage
CREATE OR REPLACE STORAGE INTEGRATION az_integr_ukcrime
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '*********'
  STORAGE_ALLOWED_LOCATIONS = ('azure://ukcrimedata.blob.core.windows.net/crime-data-files/');

DESC STORAGE INTEGRATION az_integr_ukcrime;

-- Create notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION az_integr_ukcrime_notification
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://ukcrimequeue.queue.core.windows.net/stg-crime-queue'
  AZURE_TENANT_ID = '***************';

-- Authorise the access to storage queue
DESC NOTIFICATION INTEGRATION az_integr_ukcrime_notification;

-- Create stage
USE SCHEMA DB_UK_CRIME.public;
CREATE OR REPLACE STAGE stage_uk_crime
  URL = 'azure://ukcrimedata.blob.core.windows.net/crime-data-file/'
  STORAGE_INTEGRATION = az_integr_ukcrime;

-- Define file format
CREATE OR REPLACE FILE FORMAT uk_crime_csv
TYPE = 'CSV'
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Create Snowpipe for automated ingestion
CREATE OR REPLACE PIPE DB_UK_CRIME.public.pipe_to_tb
  AUTO_INGEST = true
  INTEGRATION = 'AZ_INTEGR_UKCRIME_NOTIFICATION'
  AS
  COPY INTO DB_UK_CRIME.public.TB_UK_CRIME
  FROM @DB_UK_CRIME.public.stage_uk_crime
  FILE_FORMAT = (FORMAT_NAME = 'uk_crime_csv'); --FILE_FORMAT = (type = 'CSV');

-- Check the pipeline status (the last activity)
SELECT system$pipe_status('DB_UK_CRIME.public.pipe_to_tb');
