import os
import pydata_google_auth
from google.cloud import bigquery
from dataclasses import dataclass
from pandas import DataFrame, read_gbq
from dotenv import load_dotenv, find_dotenv
from geopandas import GeoDataFrame, GeoSeries

load_dotenv(find_dotenv())


def connect_to_gcloud(*args, **kwargs) -> bigquery.Client:
    """
    Connect to BigQuery:
        1. Prefer the user-provided service account JSON file
        2. If none provided, read from environment
        3. Otherwise, connect normally
    """

    if "GCP_SERVICE_ACCOUNT_JSON" in kwargs:
        json_path = kwargs["GCP_SERVICE_ACCOUNT_JSON"]
        kwargs.pop("GCP_SERVICE_ACCOUNT_JSON")
        return bigquery.Client.from_service_account_json(json_path, *args, **kwargs)

    GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if GCP_SERVICE_ACCOUNT_JSON:
        return bigquery.Client.from_service_account_json(
            GCP_SERVICE_ACCOUNT_JSON, *args, **kwargs
        )

    else:
        return bigquery.Client(*args, **kwargs)


def pydata_credentials(GCP_SERVICE_ACCOUNT_JSON: str | None = None):
    """Create credentials that are used by `pandas.read_gqb()`"""

    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    ]

    # Use argument if provided
    if GCP_SERVICE_ACCOUNT_JSON:
        return pydata_google_auth.load_service_account_credentials(
            GCP_SERVICE_ACCOUNT_JSON,
            scopes,
        )

    # Try an environment variable next
    GCP_SERVICE_ACCOUNT_JSON = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if GCP_SERVICE_ACCOUNT_JSON:
        return pydata_google_auth.load_service_account_credentials(
            GCP_SERVICE_ACCOUNT_JSON,
            scopes,
        )

    # Otherwise, connect as a user
    return pydata_google_auth.get_user_credentials(
        scopes,
        # Set auth_local_webserver to True to have a slightly more convienient
        # authorization flow. Note, this doesn't work if you're running from a
        # notebook on a remote sever, such as over SSH or with Google Colab.
        auth_local_webserver=True,
    )


@dataclass
class BQWarehouse:

    project_id: str
    bq_connection: dict = {}
    credentials = None
    client = None

    @property
    def pydata_creds(self):
        if not self.credentials:
            if "GCP_SERVICE_ACCOUNT_JSON" in self.bq_connection:
                self.credentials = pydata_credentials(
                    self.bq_connection["GCP_SERVICE_ACCOUNT_JSON"]
                )
            else:
                self.credentials = pydata_credentials()

        return self.credentials

    @property
    def gcloud_client(self):
        if not self.client:
            self.client = connect_to_gcloud(**self.bq_connection)

        return self.client

    ## READ data from bigquery
    ## -----------------------

    def read_df(self, query: str, **kwargs) -> DataFrame:
        """
        Read `query` into a `pd.DataFrame` with
        any optional kwargs defined here:
        https://pandas.pydata.org/docs/reference/api/pandas.read_gbq.html

        Arguments:
            query: str

        Returns:
            pd.DataFrame
        """
        return read_gbq(
            query=query,
            project_id=self.project_id,
            credentials=self.pydata_creds,
            **kwargs,
        )

    def read_gdf(
        self,
        query: str,
        geom_col: str = "geom",
        read_kwargs: dict = {},
        gdf_kwargs: dict = {},
    ) -> GeoDataFrame:
        df = self.read_df(query=query, **read_kwargs)
        df[geom_col] = GeoSeries.from_wkt(df[geom_col])

        return GeoDataFrame(
            df,
            geometry=geom_col,
            **gdf_kwargs,
        )

    ## WRITE data to bigquery
    ## ----------------------

    def write_df(
        self,
        df: DataFrame,
        destination: str,
        **kwargs,
    ) -> None:
        job = self.gcloud_client.load_table_from_dataframe(
            df,
            destination=destination,
            **kwargs,
        )
        return job.result()

    ## TODO: execute query
