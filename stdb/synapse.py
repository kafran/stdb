import typing as t
import urllib.parse
import uuid
from bisect import bisect
from collections import namedtuple
from contextlib import closing, contextmanager
from enum import Enum
from operator import attrgetter
from urllib.parse import urljoin, urlunparse

import pyspark.sql.functions as F
from azure.identity import ClientSecretCredential
from databricks.sdk.runtime import dbutils

if t.TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Add Azure Data Lake Store scheme to urllib.parse
ADLS_SCHEME = "abfss"
urllib.parse.uses_relative.append(ADLS_SCHEME)
urllib.parse.uses_netloc.append(ADLS_SCHEME)

JDBCConnection = JDBCStatement = JDBCResultSet = t.Any


class FileType(Enum):
    PARQUET = "PARQUET"
    DELTA = "DELTA"


AdlsTable = namedtuple("AdlsTable", ("url", "schema"))
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
    ):
        self.spark = spark_session
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
        self._DBFS_MOUNT_CONFIG = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{client_id}",
            "fs.azure.account.oauth2.client.secret": f"{client_secret}",
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        }
        self._VARCHAR_THRESHOLD = (
            [VarcharSize(f"varchar({i})", i) for i in range(100, 1100, 100)] + 
            [VarcharSize(f"varchar({i})", i) for i in range(2000, 10000, 2000)]
        )  # fmt: skip

    @property
    def token(self):
        return self.credential.get_token("https://database.windows.net/.default").token

    def _generate_mount_point(self):
        hex = uuid.uuid4().hex[:8]
        return f"/mnt/stdb_{hex}"

    @contextmanager
    def dbfs_mount(self, source):
        """
        Context manager to handle DBFS mount and unmount operations.

        Args:
            source (str): The DBFS source to mount.

        Yields:
            str: The mount point of the DBFS source.

        This function checks if the specified DBFS source is already
        mounted. If not, it mounts the source to a new mount point. The
        mount point is yielded for use within the context. Upon exiting
        the context, the function unmounts the source if it was newly
        mounted.
        """
        try:
            mounts = dbutils.fs.mounts()
            mount_point = next((m.mountPoint for m in mounts if m.source == source), None)
            new_mount_point = None
            if not mount_point:
                mount_point = new_mount_point = self._generate_mount_point()
                dbutils.fs.mount(
                    source=source,
                    mount_point=mount_point,
                    extra_configs=self._DBFS_MOUNT_CONFIG,
                )
            yield mount_point
        finally:
            if new_mount_point:
                dbutils.fs.unmount(new_mount_point)

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

    def build_adls_url(self, container: str, storage_account: str, path: str):
        return urlunparse(
            (
                ADLS_SCHEME,  # scheme
                f"{container}@{storage_account}.dfs.core.windows.net",  # netloc
                f"{path}",  # path
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

    def infer_varchar_size(self, schema): ...

    def get_adls_tables(
        self,
        container: str,
        storage_account: str,
        path: str,
    ) -> list[AdlsTable]:
        adls_url = self.build_adls_url(container, storage_account, path)
        with self.dbfs_mount(adls_url) as mount_point:
            files = dbutils.fs.ls(mount_point)
            adls_files = {}
            for file in files:
                # TODO: Add support for Delta Tables
                if file.name.endswith(".parquet"):
                    table_url = urljoin(adls_url, file.name)
                    adls_files[file] = AdlsTable(url=table_url, schema={})

            with self.synapse_jdbc_connection() as con:
                for file, table in adls_files.items():
                    synapse_schema = self.infer_synapse_schema(con, table.url)
                    varchar_cols = {
                        col: f"`{col}`"  # escape col name for spark read in case of special characters
                        for col, type in synapse_schema.items()
                        if "varchar" in type
                    }
                    df = self.spark.read.parquet(file.path).select(*varchar_cols.values())
                    max_length_df = df.select(
                        [
                            F.max(F.length(F.col(escape_col))).alias(col)
                            for col, escape_col in varchar_cols.items()
                        ]
                    )
                    max_lengths = [ColumnLength(*i) for i in max_length_df.collect()[0].asDict().items()]  # fmt: skip
                    max_lengths.sort(key=by_length)
                    varchar_sizes = {col.column_name: self._find_varchar_size(col.length) for col in max_lengths}  # fmt: skip
                    synapse_schema.update(varchar_sizes)
                    table.schema.update(synapse_schema)
        return list(adls_files.values())

    def create_external_table(
        self,
        storage_account,
        container,
        path,
        synapse_db,
        synapse_schema="dbo",
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
        if not path.endswith("/"):
            # TODO: Define error types
            raise ValueError("""Path must ends with "/" and point to a folder (database) on Azure Data Lake Store Gen2""")  # fmt: skip

        adls_tables = self.get_adls_tables(container, storage_account, path)

        return adls_tables
