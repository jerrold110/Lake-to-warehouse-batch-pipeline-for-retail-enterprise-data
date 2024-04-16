from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

current_date = datetime.now().date()
previous_date = current_date - timedelta(days=1)
prev_date_string = previous_date.strftime("%Y-%m-%d")

with DAG(
    "a_retail_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'

    },
    description="Data lake to warehouse batch pipeline DAG",
    schedule_interval='0 3 * * *', # Runs at 3am everyday
    start_date=datetime(2007, 6, 1),
    catchup=False,
    concurrency=2,
    tags=["spark", "hadoop"],
) as dag:
    # Data validate tasks
    val_1 = BashOperator(
        task_id="validate_payment",
        bash_command=f"docker exec spark_master ./bin/spark-submit ./pyspark_scripts/validate_data.py --file payment.csv --batch_date {prev_date_string}",
        retries=1,
        retry_delay=timedelta(minutes=0.5),
        depends_on_past=False
    )

    val_2 = BashOperator(
        task_id="validate_rental",
        bash_command=f"docker exec spark_master ./bin/spark-submit ./pyspark_scripts/validate_data.py --file rental.csv --batch_date {prev_date_string}",
        retries=1,
        retry_delay=timedelta(minutes=0.5),
        depends_on_past=False
    )

    # Data sensor tasks

    # Data etl tasks
    etl_d_date = BashOperator(
        task_id="etl_dim_date",
        bash_command=f"docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_date.py --batch_date {prev_date_string}",
        retries=1,
        retry_delay=timedelta(minutes=0.5),
        depends_on_past=False
    )

    etl_f_sale = BashOperator(
        task_id="etl_fact_date",
        bash_command=f"docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_Fsale.py --batch_date {prev_date_string}",
        retries=1,
        retry_delay=timedelta(minutes=0.5),
        depends_on_past=False
    )

    etl_d_store_cust = BashOperator(
        task_id="etl_dim_store_customer",
        bash_command=f"docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_storecustomer.py --batch_date {prev_date_string}",
        retries=1,
        retry_delay=timedelta(minutes=0.5),
        depends_on_past=False
    )

    etl_d_film = BashOperator(
        task_id="etl_dim_film",
        bash_command=f"docker exec spark_master ./bin/spark-submit ./pyspark_scripts/etl_film.py --batch_date {prev_date_string}",
        retries=1,
        retry_delay=timedelta(minutes=0.5),
        depends_on_past=False
    )

    ##
    end = BashOperator(
        task_id="End",
        bash_command=f"Pipeline complete batch: {prev_date_string}"
    )


    [val_1, val_2] >> etl_d_date >> etl_f_sale
    etl_d_film >> etl_f_sale
    etl_d_store_cust >> etl_f_sale
    etl_f_sale >> end

