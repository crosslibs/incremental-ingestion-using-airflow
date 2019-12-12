# Periodic ingestion of incremental data into BigQuery using Cloud Composer (Apache Airflow)

The repository showcases an orchestration workflow for ingesting incremental updates to data in BigQuery table using Apache Airflow / Cloud Composer.

## Airflow / Composer DAG Design
![Airflow - Incremental Updates](https://user-images.githubusercontent.com/20769938/70675379-06f81e80-1caf-11ea-9738-67fac2872c23.png)
### Explanation
The orchestration workflow (DAG) executes the following tasks in sequence:
* Fetches configuration (specified in `yaml`) from Airflow data folder or local filesystem using `PythonOperator`  (task id: `load_config`)
* Creates a staging table in BQ to ingest the incremental updates using `BigQueryCreateEmptyTableOperator`  (task id: `bq_create_staging`)
* Using Dataflow template [`JDBC to BigQuery`](https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#jdbctobigquery), the data is staged into the staging table. This task is carried out using `DataflowTemplateOperator` (task id: `stage_data`)
* Now using [BigQuery MERGE statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement), the updates in the staging table are merged with the final table. This task is done using `BigQueryOperator` (task id: `bq_merge`).
* Once the merge task completes, the staging table is deleted using `BigQueryTableDeleteOperator` (task id: `bq_delete_staging`)
* Finally, the configuration stored in Cloud Composer / Apache Airflow environment variables is also cleaned up using `PythonOperator` (task id: `delete_config`)

## Steps to run the DAG

### 1. Upload DAG
Upload the DAG by copying into `~/airflow/dags` folder if your are using `Apache Airflow` or into Cloud Composer DAGS folder.

### 2. Create configuration

Create configuration for the job that captures the BQ and Datflow configuration parameters needed in a YAML file. Please refer to `example-config.yaml` in the repo for a sample configuration file.

In addition, create a BigQuery table schema file and add it to a GCS bucket / folder. Please refer to `example-schema.json` for a sample.

### 3. Upload configuration

If you are using Cloud Composer, copy the configuration file into `data` folder in the environemnt GCS bucket.

### 4. Run the DAG

If you are using Cloud Composer, you can trigger the DAG anytime using the following `gcloud` command:

```
> gcloud composer environments run <environment-name> --location <gcp-location> trigger_dag -- incremental_ingestion -c '{"file": "/home/airflow/gcs/data/example-config.yaml"}' -r <run-id> 
```

If you are using Apache Airflow, you can trigger the same using the following `airflow` command:

```
> airflow trigger_dag incremental_ingestion -c '{"file": "/path/to/example-config.yaml"}' -r <run-id> 
```