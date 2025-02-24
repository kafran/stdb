import re
import typing as T
import unicodedata
import urllib.parse as P
import uuid
from bisect import bisect
from collections import namedtuple
from contextlib import closing, contextmanager
from enum import Enum
from operator import attrgetter

import pyspark.sql.functions as F
from azure.identity import ClientSecretCredential
from databricks.sdk.runtime import dbutils

if T.TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Add Azure Data Lake Store scheme to urllib.parse
ADLS_SCHEME = "abfss"
P.uses_relative.append(ADLS_SCHEME)
P.uses_netloc.append(ADLS_SCHEME)

JDBCConnection = JDBCStatement = JDBCResultSet = T.Any


class FileType(Enum):
    PARQUET = "PARQUET"
    DELTA = "DELTA"


AdlsTable = namedtuple("AdlsTable", ("file", "schema"))
VarcharSize = namedtuple("VarcharSize", ("varchar_size", "length"))
ColumnLength = namedtuple("ColumnLength", ("column_name", "length"))
by_length = attrgetter("length")


class Synapse:
    def __init__(
        self,
        spark_session: "SparkSession",
        tenant_id,
        client_id,
        client_secret,
        synapse_workspace,
        storage_account,
    ):
        self.spark = spark_session
        self.storage_account = storage_account
        self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")  # fmt: skip
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")  # fmt: skip
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", f"{client_id}")  # fmt: skip
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", f"{client_secret}")  # fmt: skip
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")  # fmt: skip
        self.credential = ClientSecretCredential(
            tenant_id,
            client_id,
            client_secret,
        )
        self._SQL_POOL_URL = (
            "jdbc:sqlserver://"
            f"{synapse_workspace}-ondemand.sql.azuresynapse.net"
            ":1433"
            ";encrypt=true;trustServerCertificate=false"
            ";hostNameInCertificate=*.sql.azuresynapse.net"
            ";loginTimeout=30"
        )
        self._VARCHAR_THRESHOLD = (
            [VarcharSize(f"varchar({i})", i) for i in range(100, 1100, 100)] + 
            [VarcharSize(f"varchar({i})", i) for i in range(2000, 10000, 2000)]
        )  # fmt: skip

    @property
    def token(self):
        return self.credential.get_token("https://database.windows.net/.default").token

    @contextmanager
    def synapse_jdbc_connection(self) -> JDBCConnection:
        """
        Context manager for establishing a JDBC connection to the
        configured Synapse Workspace.

        The connection is yielded to the caller and automatically closed
        upon exit.

        Yields:
        con: A JDBC connection object.

        Example:
            with self.synapse_jdbc_connection() as con:
                # Use the connection
                pass
        """
        properties = self.spark._sc._gateway.jvm.java.util.Properties()  # type: ignore
        properties.setProperty("accessToken", self.token)  # type: ignore
        driver_manager = self.spark._sc._gateway.jvm.java.sql.DriverManager  # type: ignore
        con = driver_manager.getConnection(self._SQL_POOL_URL, properties)  # type: ignore
        try:
            yield con
        finally:
            con.close()  # type: ignore

    @contextmanager
    def jdbc_statement(self, con: JDBCConnection) -> JDBCStatement:
        stmt = con.createStatement()
        try:
            yield stmt
        finally:
            stmt.close()

    # TODO : Change for contextlib.closing
    @contextmanager
    def jdbc_resultset(self, stmt: JDBCStatement, query) -> JDBCResultSet:
        result = stmt.execute(query)
        if result:
            result_set = stmt.getResultSet()
            try:
                yield result_set
            finally:
                result_set.close()

    def build_adls_url(self, container: str, storage_account: str, path=""):
        return P.urlunparse(
            (
                ADLS_SCHEME,  # scheme
                f"{container}@{storage_account}.dfs.core.windows.net",  # netloc
                path,  # path
                "",  # params
                "",  # query
                "",  # fragment
            )
        )

    def sql_to_infer_schema(
        self,
        adls_url: str,
        type: FileType,
    ) -> str:
        INFER_SCHEMA_TEMPL = (
            "EXEC sp_describe_first_result_set N'"
            "SELECT TOP 0 * FROM "
            "OPENROWSET("
            f"BULK ''{adls_url}'', "
            f"FORMAT=''{type.value}''"
            ") AS [r]';"
        )
        return INFER_SCHEMA_TEMPL

    def infer_synapse_schema(
        self,
        con: JDBCConnection,
        adls_url: str,
        type=FileType.PARQUET,
    ) -> dict[str, str]:
        query = self.sql_to_infer_schema(adls_url, type)
        with self.jdbc_statement(con) as stmt:
            is_resultset = stmt.execute(query)
            if is_resultset:
                with closing(stmt.getResultSet()) as resultset:
                    data = {}
                    while resultset.next():
                        col_name = resultset.getObject("name")
                        col_type = resultset.getObject("system_type_name")
                        data[col_name] = col_type
                    return data
            return {}  # TODO: Maybe throw?

    def _find_varchar_size(self, length):
        threshold = self._VARCHAR_THRESHOLD
        if length < 50:
            return "varchar(50)"
        idx = bisect(threshold, length, key=by_length)
        if idx == len(threshold):
            return "varchar(max)"
        else:
            return threshold[idx].varchar_size

    # TODO: Add support for Delta Tables
    def get_adls_tables(
        self,
        container: str,
        storage_account: str,
        path: str,
    ) -> list[AdlsTable]:
        path = path.rstrip("/")
        adls_url = self.build_adls_url(container, storage_account, path)
        try:
            files = dbutils.fs.ls(adls_url)
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                raise ValueError(f"""java.io.FileNotFoundException at {adls_url}""")  # fmt: skip
            else:
                raise
        if path.endswith(".parquet"):
            if len(files) > 1:
                filename = path.split("/")[-1]
                path = "/".join(path.split("/")[:-1])
                adls_url = self.build_adls_url(container, storage_account, path)
                files = dbutils.fs.ls(adls_url)
                files = [file for file in files if file.name == f"{filename}/"]
        adls_tables = []
        for file in files:
            table = AdlsTable(file=file, schema={})
            with self.synapse_jdbc_connection() as con:
                synapse_schema = self.infer_synapse_schema(con, file.path)
                for col, type in synapse_schema.items():
                    if "varbinary" in type:
                        synapse_schema[col] = "varbinary(max)"
                varchar_cols = {
                    col: f"`{col}`"  # escape col name for spark read in case of special characters
                    for col, type in synapse_schema.items()
                    if "varchar" in type
                }
                if not varchar_cols:
                    table.schema.update(synapse_schema)
                    adls_tables.append(table)
                    continue
                try:
                    df = self.spark.read.parquet(file.path).select(*varchar_cols.values())
                    max_length_df = df.select(
                        [
                            F.coalesce(
                                F.max(F.length(F.col(escape_col))),
                                F.lit(0),  # treat null as zero
                            ).alias(col)
                            for col, escape_col in varchar_cols.items()
                        ]
                    )
                    max_lengths = [ColumnLength(*i) for i in max_length_df.collect()[0].asDict().items()]  # fmt: skip
                    max_lengths.sort(key=by_length)
                    varchar_sizes = {col.column_name: self._find_varchar_size(col.length) for col in max_lengths}  # fmt: skip
                    synapse_schema.update(varchar_sizes)
                    table.schema.update(synapse_schema)
                    adls_tables.append(table)
                except Exception as e:
                    raise Exception(f"Error processing file: {file.path}") from e
        return adls_tables

    def _generate_data_source_name(self, url):
        parsed_url = P.urlparse(url)
        return parsed_url.netloc.replace("@", "_").replace(".", "_")

    def _generate_file_format_name(self, format: FileType):
        return f"Synapse{format.name.capitalize()}Format"

    def _normalize_table_name(self, text):
        if text.endswith(".parquet"):
            text = text[:-8]
        text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")  # fmt:skip
        # Remove special characters
        text = re.sub(r"[^a-zA-Z0-9]", " ", text)
        # Convert CamelCase to snake_case
        text = re.sub("([a-z0-9])([A-Z])", r"\1_\2", text)
        text = text.title()
        text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
        # Convert to lower case
        text = text.lower()
        # Replace spaces with underscores
        text = re.sub(r"\s+", "_", text)
        # Remove consecutive underscores
        text = re.sub(r"_+", "_", text)
        return text

    def sql_to_create_file_format(self, type: FileType):
        file_format = self._generate_file_format_name(format=type)
        CREATE_FILE_FORMAT_TEMPL = (
            f"IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = '{file_format}') "
            f"CREATE EXTERNAL FILE FORMAT [{file_format}] "
            f"WITH (FORMAT_TYPE = {type.value});"
        )
        return CREATE_FILE_FORMAT_TEMPL

    def sql_to_create_data_source(self, url):
        data_source = self._generate_data_source_name(url)
        CREATE_DATA_SOURCE_TEMPL = (
            f"IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = '{data_source}') "
            f"CREATE EXTERNAL DATA SOURCE [{data_source}] "
            f"WITH (LOCATION = '{url}');"
        )
        return CREATE_DATA_SOURCE_TEMPL

    def sql_to_create_schema(self, db_schema: str):
        CREATE_SCHEMA_TEMPL = (
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{db_schema}') "
            "BEGIN "
            f"EXEC('CREATE SCHEMA {db_schema}'); "
            "END"
        )
        return CREATE_SCHEMA_TEMPL

    # TODO: Add support for Delta Tables
    def sql_to_create_external_table(self, db_schema: str, table: AdlsTable):
        data_source = self._generate_data_source_name(table.file.path)
        file_format = self._generate_file_format_name(format=FileType.PARQUET)
        fields = ", ".join(
            [
                f"[{column_name}] {column_type}"
                for column_name, column_type in table.schema.items()
            ]
        )
        parsed_url = P.urlparse(table.file.path)
        table_name = self._normalize_table_name(
            parsed_url.path.rstrip("/").split("/")[-1]
        )
        CREATE_TABLE_TEMPL = (
            f"CREATE EXTERNAL TABLE {db_schema}.{table_name} "
            f"({fields}) "
            f"WITH (LOCATION = '{parsed_url.path}', "
            f"DATA_SOURCE = [{data_source}], "
            f"FILE_FORMAT = [{file_format}]);"
        )
        return CREATE_TABLE_TEMPL

    def sql_to_drop_existing_table(self, db_schema: str, table: AdlsTable):
        parsed_url = P.urlparse(table.file.path)
        table_name = self._normalize_table_name(
            parsed_url.path.rstrip("/").split("/")[-1]
        )
        DROP_EXISTING_TABLE_TEMPL = (
            f"IF EXISTS (SELECT 1 FROM sys.external_tables WHERE name = '{table_name}' "
            f"AND schema_id = SCHEMA_ID('{db_schema}')) "
            "BEGIN "
            f"DROP EXTERNAL TABLE [{db_schema}].[{table_name}]; "
            "END"
        )
        return DROP_EXISTING_TABLE_TEMPL

    def create_external_table(
        self,
        storage_account,
        container,
        path,
        synapse_db,
        synapse_schema="dbo",
        drop_existing_table=True,
    ):
        """
        Creates an external table in Azure Synapse from Parquet files
        stored in ADLS Gen2.

        Parameters:
        storage_account (str): The name of the Azure Storage account.
        container (str): The name of the container in the storage account.
        path (str): The path to the Parquet files within the container.
        synapse_db (str): The name of the Synapse SQL database where the
        external table will be created.
        synapse_schema (str, optional): The schema name under which the
        external table will be created. Defaults to 'dbo'.

        Returns:
        None

        Example:
        create_external_table(
            storage_account="mystorageaccount",
            container="mycontainer",
            path="data/parquetfiles",
            synapse_db="mysynapsedb"
        )
        """
        adls_tables = self.get_adls_tables(container, storage_account, path)

        with self.synapse_jdbc_connection() as con:
            # Set the database on Synapse
            with self.jdbc_statement(con) as stmt:
                stmt.execute(f"USE {synapse_db};")
            # Create the schema if not exist
            with self.jdbc_statement(con) as stmt:
                stmt.execute(self.sql_to_create_schema(synapse_schema))
            # Create the data source if not exist
            with self.jdbc_statement(con) as stmt:
                data_source = self.build_adls_url(container, storage_account)
                stmt.execute(self.sql_to_create_data_source(data_source))
            # Create the file format if not exist
            with self.jdbc_statement(con) as stmt:
                stmt.execute(self.sql_to_create_file_format(FileType.PARQUET))
            # Create the external tables
            for table in adls_tables:
                if drop_existing_table:
                    with self.jdbc_statement(con) as stmt:
                        stmt.execute(self.sql_to_drop_existing_table(synapse_schema, table))  # fmt:skip
                with self.jdbc_statement(con) as stmt:
                    stmt.execute(self.sql_to_create_external_table(synapse_schema, table))
