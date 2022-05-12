import pydata_google_auth
from google.cloud import bigquery


def pydata_auth():
    # https://pandas-gbq.readthedocs.io/en/latest/howto/authentication.html#authenticating-with-a-user-account
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    ]
    return pydata_google_auth.get_user_credentials(
        SCOPES,
        # Set auth_local_webserver to True to have a slightly more convienient
        # authorization flow. Note, this doesn't work if you're running from a
        # notebook on a remote sever, such as over SSH or with Google Colab.
        auth_local_webserver=True,
    )


def gcloud_auth():
    return bigquery.Client()
