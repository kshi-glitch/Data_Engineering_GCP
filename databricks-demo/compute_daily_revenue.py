# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Compute_Daily_Revenue_Pipeline = Job.from_dict(
    {
        "name": "Compute Daily Revenue Pipeline",
        "tasks": [
            {
                "task_key": "cleanup_retail_data_lake",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/pkshitij025@gmail.com/ELT Data Pipeline for Daily Product Revenue/01 Cleanup Database and Datasets",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "convert_csv_to_parquet_orders",
                "depends_on": [
                    {
                        "task_key": "cleanup_retail_data_lake",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/pkshitij025@gmail.com/ELT Data Pipeline for Daily Product Revenue/02 File Format Converter",
                    "base_parameters": {
                        "ds": "orders",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "create_table_orders",
                "depends_on": [
                    {
                        "task_key": "convert_csv_to_parquet_orders",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/pkshitij025@gmail.com/ELT Data Pipeline for Daily Product Revenue/03 Create Spark SQL Tables",
                    "base_parameters": {
                        "table_name": "orders",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "csv_to_parquet_order_items",
                "depends_on": [
                    {
                        "task_key": "cleanup_retail_data_lake",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/pkshitij025@gmail.com/ELT Data Pipeline for Daily Product Revenue/02 File Format Converter",
                    "base_parameters": {
                        "ds": "order_items",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "create_table_order_items",
                "depends_on": [
                    {
                        "task_key": "csv_to_parquet_order_items",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/pkshitij025@gmail.com/ELT Data Pipeline for Daily Product Revenue/03 Create Spark SQL Tables",
                    "base_parameters": {
                        "table_name": "order_items",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "compute_daily_product_revenue",
                "depends_on": [
                    {
                        "task_key": "create_table_order_items",
                    },
                    {
                        "task_key": "create_table_orders",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/pkshitij025@gmail.com/ELT Data Pipeline for Daily Product Revenue/04 Daily Product Revenue",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "15.4.x-scala2.12",
                    "gcp_attributes": {
                        "use_preemptible_executors": False,
                        "availability": "ON_DEMAND_GCP",
                        "zone_id": "HA",
                    },
                    "node_type_id": "n2-highmem-4",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                    },
                    "enable_elastic_disk": False,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 8,
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Compute_Daily_Revenue_Pipeline, job_id=401771174739346)
# or create a new job using: w.jobs.create(**Compute_Daily_Revenue_Pipeline.as_shallow_dict())
