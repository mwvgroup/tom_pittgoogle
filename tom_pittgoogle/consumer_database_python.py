#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to manage BigQuery connections via Python client, and work with data.

BigQuery Python Client docs: https://googleapis.dev/python/bigquery/latest/index.html

Used by `BrokerDatabasePython`, but can be called independently.

Basic workflow:

.. code:: python

    consumer = ConsumerDatabasePython(table_name)

    sql_stmnt, job_config = consumer.create_sql_stmnt(parameters)
    query_job = consumer.client.query(sql_stmnt, job_config=job_config)

    alerts = consumer.unpack_query(query_job)  # List[dict]

See especially:

.. autosummary::
   :nosignatures:

   ConsumerDatabasePython.authenticate
   ConsumerDatabasePython.create_sql_stmnt
   ConsumerDatabasePython.unpack_query

"""

from django.conf import settings
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import logging as gc_logging
from google_auth_oauthlib.helpers import credentials_from_session
from requests_oauthlib import OAuth2Session


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class ConsumerDatabasePython:
    """Consumer class to query alerts from BigQuery, and manipulate them.

    Initialization does the following:

        - Authenticate the user via OAuth 2.0.

        - Create a `google.cloud.bigquery.Client` object for the user/broker
          to query database with.

        - Check that the table exists and we can connect.

    To view logs, visit: https://console.cloud.google.com/logs

        - Make sure you are logged in, and your project is selected in the dropdown
          at the top.

        - Click the "Log name" dropdown and select the table name you instantiate this
          consumer with.

    TODO: Give the user a standard logger.
    """

    def __init__(self, table_name):
        """Authenticate user; create a client, the table path and check connection."""
        user_project = settings.GOOGLE_CLOUD_PROJECT

        self.authenticate()
        self.credentials = credentials_from_session(self.oauth2)
        self.client = bigquery.Client(
            project=user_project, credentials=self.credentials
        )

        # logger
        log_client = gc_logging.Client(
            project=user_project, credentials=self.credentials
        )
        self.logger = log_client.logger(table_name)

        # table
        self.table_name = table_name
        self.table_path = f"{PITTGOOGLE_PROJECT_ID}.{table_name}"
        self._get_table()

        # for the TOM `GenericAlert`. this won't be very helpful without instructions.
        self.query_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{user_project}/queries"

    def authenticate(self):
        """Guide user through authentication; create `OAuth2Session` for credentials.

        The user will need to visit a URL, authenticate themselves, and authorize
        `PittGoogleConsumer` to make API calls on their behalf.

        The user must have a Google account that is authorized make API calls
        through the project defined by the `GOOGLE_CLOUD_PROJECT` variable in the
        Django `settings.py` file. Any project can be used, as long as the user has
        access.

        Additional requirement because this is still in dev: The OAuth is restricted
        to users registered with Pitt-Google, so contact us.

        TODO: Integrate this with Django. For now, the user interacts via command line.
        """
        # create an OAuth2Session
        client_id = settings.PITTGOOGLE_OAUTH_CLIENT_ID
        client_secret = settings.PITTGOOGLE_OAUTH_CLIENT_SECRET
        authorization_base_url = "https://accounts.google.com/o/oauth2/auth"
        redirect_uri = "https://ardent-cycling-243415.appspot.com/"  # TODO: better page
        scopes = [
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/bigquery",
        ]
        oauth2 = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)

        # instruct the user to authorize
        authorization_url, state = oauth2.authorization_url(
            authorization_base_url,
            access_type="offline",
            # access_type="online",
            # prompt="select_account",
        )
        print((
            "Please visit this URL to authenticate yourself and authorize "
            "PittGoogleConsumer to make API calls on your behalf:"
            f"\n\n{authorization_url}\n"
        ))
        authorization_response = input(
            "After authorization, you should be directed to the Pitt-Google Alert "
            "Broker home page. Enter the full URL of that page (it should start with "
            "https://ardent-cycling-243415.appspot.com/):\n"
        )

        # complete the authentication
        _ = oauth2.fetch_token(
            "https://accounts.google.com/o/oauth2/token",
            authorization_response=authorization_response,
            client_secret=client_secret,
        )
        self.oauth2 = oauth2

    def _get_table(self):
        """Make sure the resource exists, and we can connect to it."""
        try:
            _ = self.client.get_table(self.table_path)
        except NotFound as e:
            self._log_and_print(
                f"Table not found. Can't connect to {self.table_path}", severity="DEBUG"
            )
            raise e
        else:
            self._log_and_print(f"Successfully connected to {self.table_path}")

    def create_sql_stmnt(self, parameters):
        """Create the SQL statement and a job config with the user's `parameters`."""
        query_parameters = []

        select = """
            SELECT
                publisher,
                objectId,
                candid,
                CASE candidate.fid
                    WHEN 1 THEN 'g' WHEN 2 THEN 'R' WHEN 3 THEN 'i' END as filter,
                candidate.magpsf as mag,
                candidate.jd as jd,
                candidate.ra as ra,
                candidate.dec as dec,
                candidate.classtar as classtar
        """

        frm = f"FROM `{self.table_path}`"

        where = ""
        if parameters["objectId"]:
            where = "WHERE objectId=@objectId"
            query_parameters.append(
                bigquery.ScalarQueryParameter(
                    "objectId", "STRING", parameters["objectId"]
                )
            )
        elif parameters["candid"]:
            where = "WHERE candid=@candid"
            query_parameters.append(
                bigquery.ScalarQueryParameter("candid", "INT64", parameters["candid"])
            )

        orderby = "ORDER BY jd DESC"

        limit = "LIMIT @max_results"
        query_parameters.append(
            bigquery.ScalarQueryParameter(
                "max_results", "INT64", parameters["max_results"]
            )
        )

        # construct the SQL statement and job config
        sql_stmnt = " ".join([select, frm, where, orderby, limit])
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        return sql_stmnt, job_config

    def unpack_query(
        self, query_job, callback=None, **kwargs
    ):
        """Unpack alerts from `query_job`; run `callback` if present.

        A basic filter is implemented directly in the SQL statement produced by
        `create_sql_stmnt`. More complex filters could be implemented here via a
        `callback` function.
        """
        alerts = []
        for row in query_job.result():
            alert_dict = dict(row)

            if callback is not None:
                alert_dict = callback(alert_dict, **kwargs)

            if alert_dict is not None:
                alerts.append(alert_dict)

        return alerts

    def _log_and_print(self, msg, severity="INFO"):
        self.logger.log_text(msg, severity=severity)
        print(msg)
