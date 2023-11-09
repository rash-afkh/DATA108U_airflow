from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List, Dict


class DataQualityOperator(BaseOperator):
    """
    Performs data quality checks on Redshift using SQL queries.

    :param dq_checks: A list of data quality checks to perform, each defined as a dictionary with 'check_sql' and 'expected_result' keys.
    :type dq_checks: List[Dict[str, str]]
    :param redshift_conn_id: The Redshift connection ID.
    :type redshift_conn_id: str
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self, dq_checks: List[Dict[str, str]], redshift_conn_id: str, *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        if not self.dq_checks:
            self.log.info("No data quality checks provided")
            return

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []

        for check in self.dq_checks:
            sql = check.get("check_sql")
            exp_result = check.get("expected_result")

            try:
                self.log.info(f"Running query: {sql}")
                records = redshift_hook.get_first(sql)
            except Exception as e:
                self.log.error(f"Query failed with exception: {e}")
                error_count += 1
                continue

            if exp_result != records:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info("Tests failed")
            self.log.info(failing_tests)
            raise ValueError("Data quality check failed")
        else:
            self.log.info("All data quality checks passed")
