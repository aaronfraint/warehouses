from dataclasses import dataclass
from pandas import DataFrame, read_gbq
from geopandas import GeoDataFrame, GeoSeries

from .auth import pydata_auth, gcloud_auth


@dataclass
class BQWarehouse:

    project_id: str
    credentials = None
    client = None

    @property
    def pydata_creds(self):
        if not self.credentials:
            self.credentials = pydata_auth()

        return self.credentials

    @property
    def gcloud_client(self):
        if not self.client:
            self.client = gcloud_auth()

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
