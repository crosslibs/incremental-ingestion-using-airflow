# Copyright 2019 Chaitanya Prakash N <cp@crosslibs.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#!/usr/bin/python


import datetime as dt
import json
from uuid import uuid4

import jinja2
import yaml
from airflow import DAG, AirflowException
from airflow.configuration import conf
from airflow.contrib.operators.bigquery_operator import (
    BigQueryCreateEmptyTableOperator, BigQueryOperator)
from airflow.contrib.operators.bigquery_table_delete_operator import \
    BigQueryTableDeleteOperator
from airflow.contrib.operators.dataflow_operator import \
    DataflowTemplateOperator
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from jsonschema import ValidationError, validate

"""
    Validate configuration schema
"""
CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "bigquery": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string"},
                "dataset_id": {"type": "string"},
                "table_id": {"type": "string"},
                "staging_table_id": {"type": "string"},
                "schema_file": {"type": "string"},
                "merge": {
                    "type": "object",
                    "properties": {
                        "condition": {"type": "string"},
                        "matched": {"type": "string"},
                        "notmatched": {"type": "string"}
                    },
                    "required": ["condition", "matched", "notmatched"]
                }
            },
            "required": ["table_id", "merge", "schema_file"]
        },
        "dataflow": {
            "type": "object",
            "properties": {
                "template": {"type": "string"},
                "job_name_prefix": {"type": "string"},
                "jobname": {"type": "string"},
                "options": {"type": "object"},
                "parameters": {"type": "object"}
            },
            "required": ["template", "parameters", "job_name_prefix"]
        }
    },
    "required": ["bigquery", "dataflow"]
}

run_config = None
YESTERDAY = dt.datetime.now() - dt.timedelta(days=1)

default_args = {
    'owner': 'Incremental Data Ingestion Pipeline',
    'depends_on_past': False,
    'email': ['cp@crosslibs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'start_date': YESTERDAY
}


def fetch_config(ds, **kwargs):
    CONFIG_PARAM, config = 'file', None
    conf_file = kwargs['dag_run'].conf[CONFIG_PARAM]
    if conf_file is None or conf_file.strip() == '':
        raise AirflowException(
            'Config parameter {} is not specified.'.format(CONFIG_PARAM))
    print('Config file for the job: {}'.format(conf_file))
    print('Reading configuration from {}'.format(conf_file))
    try:
        with open(conf_file, "r") as f:
            config = yaml.load(f)
    except Exception as error:
        raise AirflowException(
            'Error while reading the config file: {}'.format(error))
    try:
        validate(instance=config, schema=CONFIG_SCHEMA)
    except ValidationError as error:
        raise AirflowConfigException(
            'Invalid configuration specified: {}'.format(error))
    if 'staging_table_id' not in config['bigquery']:
        config['bigquery']['staging_table_id'] = 'staging_{}'.format(str(uuid4())[
                                                                     :8])
    config['bigquery']['merge_table'] = '{}:{}.{}'.format(
        config['bigquery']['project_id'], config['bigquery']['dataset_id'], config['bigquery']['table_id'])
    config['bigquery']['staging_table'] = '{}:{}.{}'.format(
        config['bigquery']['project_id'], config['bigquery']['dataset_id'], config['bigquery']['staging_table_id'])
    if 'jobname' not in config['dataflow']:
        config['dataflow']['jobname'] = '{}-{}'.format(
            config['dataflow']['job_name_prefix'], str(uuid4())[:8])
    config['bigquery']['merge_query'] = 'MERGE `{}` t USING `{}` s ON {} WHEN MATCHED THEN {} WHEN NOT MATCHED THEN {}'.format(config['bigquery']['merge_table'].replace(':', '.'), config['bigquery']['staging_table'].replace(':', '.'), config['bigquery']['merge']['condition'], config['bigquery']['merge']['matched'], config['bigquery']['merge']['notmatched'])
    print('Airflow config: {}'.format(config))
    config_var = 'config-{}'.format(kwargs['dag_run'].run_id)
    print('Writing config to variable: {}'.format(config_var))
    Variable.set(config_var, config, serialize_json=True)


def cleanup_config(ds, **kwargs):
    config_var = 'config-{}'.format(kwargs['dag_run'].run_id)
    print('Removing configuration from variable: {}'.format(config_var))
    # Variable.delete(key=config_var)


dag = DAG('incremental_ingestion',
          catchup=False,
          default_args=default_args,
          schedule_interval=dt.timedelta(days=1))

load_config = PythonOperator(task_id='load_config',
                             provide_context=True,
                             python_callable=fetch_config,
                             dag=dag)

bq_create_staging = BigQueryCreateEmptyTableOperator(task_id='bq_create_staging',
                                                     project_id='{{(var.json|attr("config-{}".format(run_id)))["bigquery"]["project_id"]}}',
                                                     dataset_id='{{(var.json|attr("config-{}".format(run_id)))["bigquery"]["dataset_id"]}}',
                                                     table_id='{{(var.json|attr("config-{}".format(run_id)))["bigquery"]["staging_table_id"]}}',
                                                     gcs_schema_object='{{(var.json|attr("config-{}".format(run_id)))["bigquery"]["schema_file"]}}',
                                                     dag=dag)

stage_data = DataflowTemplateOperator(task_id='stage_data',
                                      template='{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["template"] }}',
                                      dataflow_default_options={
                                          'project': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["options"]["project"] }}',
                                          'region': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["options"]["region"] }}',
                                          'zone': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["options"]["zone"] }}',
                                          'network': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["options"]["network"] }}',
                                          'subnetwork': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["options"]["subnetwork"] }}',
                                          'tempLocation': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["options"]["tempLocation"] }}',
                                      },
                                      parameters={
                                          'driverJars': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["driverJars"] }}',
                                          'driverClassName': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["driverClassName"] }}',
                                          'connectionURL': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["connectionURL"] }}',
                                          'query': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["query"] }}',
                                          'outputTable': '{{ (var.json|attr("config-{}".format(run_id)))["bigquery"]["staging_table"] }}',
                                          'bigQueryLoadingTemporaryDirectory': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["bigQueryLoadingTemporaryDirectory"] }}',
                                          'connectionProperties': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["connectionProperties"] }}',
                                          'username': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["username"] }}',
                                          'password': '{{ (var.json|attr("config-{}".format(run_id)))["dataflow"]["parameters"]["password"] }}',
                                      },
                                      dag=dag)

bq_merge = BigQueryOperator(task_id='bq_merge',
                            sql='{{(var.json|attr("config-{}".format(run_id)))["bigquery"]["merge_query"]}}',
                            use_legacy_sql=False,
                            write_disposition='WRITE_APPEND',
                            create_disposition='CREATE_IF_NEEDED',
                            dag=dag)

bq_delete_staging = BigQueryTableDeleteOperator(task_id='bq_delete_staging',
                                                deletion_dataset_table='{{(var.json|attr("config-{}".format(run_id)))["bigquery"]["staging_table"]}}',
                                                dag=dag)

delete_config = PythonOperator(task_id='delete_config',
                               provide_context=True,
                               python_callable=cleanup_config,
                               dag=dag)

load_config >> bq_create_staging >> stage_data >> bq_merge >> bq_delete_staging >> delete_config
