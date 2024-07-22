import typing as t
import uuid
from collections import namedtuple
from contextlib import contextmanager

from azure.identity import ClientSecretCredential
from databricks.sdk.runtime import dbutils

if t.TYPE_CHECKING:
    from pyspark.sql import SparkSession


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
    def synapse_jdbc_connection(self):
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
    def jdbc_statement(self, con: t.Any):
        stmt = con.createStatement()
        try:
            yield stmt
        finally:
            stmt.close()

    @contextmanager
    def jdbc_resultset(self, stmt: t.Any, query):
        result = stmt.execute(query)
        if result:
            result_set = stmt.getResultSet()
            try:
                yield result_set
            finally:
                result_set.close()

    def create_external_table(self, database, abfs, schema="dbo"):
        """
        Creates an external table in the specified database within Azure
        Synapse.

        Parameters:
        database (str): The name of the Synapse SQL database where the
        external table will be created.
        abfs (str): The ABFS (Azure Blob File System) URL where the data
        is stored: 'abfss://container@storage_account.dfs.core.windows.net/path/to/data'.
        schema (str, optional): The schema name under which the external
        table will be created. Defaults to 'dbo'.

        Returns:
        None

        Example:
        synapse = Synapse()
        synapse.create_external_table(
            database="syndb_mydb",
            abfs="abfss://container@storage_account.dfs.core.windows.net/path/to/data",
            schema="myschema"
        )
        """
        FileDetails = namedtuple("FileDetails", ["schema", "df"], defaults=({}, None))

        with self.dbfs_mount(abfs) as mount_point:
            files = dbutils.fs.ls(mount_point)
            parquet_files = {
                f: FileDetails() for f in files if f.name.endswith(".parquet")
            }

            with self.synapse_jdbc_connection() as con:
                for f in parquet_files:
                    synapse_schema = infer_parquet_schema()
                    parquet_files[f] = synapse_schema
