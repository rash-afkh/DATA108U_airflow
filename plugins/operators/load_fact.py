from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Load data into a fact table in Redshift using a SQL statement.

    :param table: The target Redshift table to load data into.
    :type table: str
    :param redshift_conn_id: The Redshift connection ID.
    :type redshift_conn_id: str
    :param load_sql_stmt: The SQL statement to use for loading data.
    :type load_sql_stmt: str
    """

    ui_color = "#F98866"

    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(
        self, table: str, redshift_conn_id: str, load_sql_stmt: str, *args, **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table, self.load_sql_stmt
        )

        self.log.info(f"Loading data into the fact table '{self.table}' in Redshift")
        redshift.run(formatted_sql)
