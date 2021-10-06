#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to manage BigQuery connections via Python client, and work with data.

Used by `PittGoogleBrokerDatabasePython`.

Typical workflow:

.. code:: python

    consumer = ConsumerStreamRest(subscription_name)

    response = consumer.oauth2.post(
        f"{consumer.subscription_url}:pull", data={"maxMessages": max_messages},
    )

    alerts = consumer.unpack_and_ack_messages(
        response, lighten_alerts=True, callback=user_filter,
    )  # List[dict]

See especially:

.. autosummary::
   :nosignatures:

   ConsumerStreamRest.authenticate
   ConsumerStreamRest.get_create_subscription
   ConsumerStreamRest.unpack_and_ack_messages

Pub/Sub REST API docs: https://cloud.google.com/pubsub/docs/reference/rest
"""

from django.conf import settings
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
# from google.oauth2.credentials import Credentials
from google_auth_oauthlib.helpers import credentials_from_session
# import json
from requests_oauthlib import OAuth2Session


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class ConsumerDatabasePython:
    """Consumer class to pull or query alerts from Pitt-Google, and manipulate them."""

    def __init__(self, table_name):
        """Construct a consumer to query the database via the BigQuery Python client.

        Open a subscriber client. If the subscription doesn't exist, create it.

        View logs:
            1. https://console.cloud.google.com
            2.

        Authentication creates an `OAuth2Session` object which can be used to fetch
        data, for example: `response = PittGoogleConsumer.oauth2.get({url})`
        """
        self.authenticate()
        self.credentials = credentials_from_session(self.oauth2)
        self.client = bigquery.Client(
            project=settings.GOOGLE_CLOUD_PROJECT, credentials=self.credentials
        )

        # logger
        # TODO: add needed params/logic

        # table
        self.table_name = table_name
        self.table_path = f"{PITTGOOGLE_PROJECT_ID}.ztf_alerts.{table_name}"
        self._get_table()

        # for the TOM `GenericAlert`. this won't be very helpful without instructions.
        self.query_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{settings.GOOGLE_CLOUD_PROJECT}/queries"

    # def _create_google_credentials(self):
    #     token_url = "https://www.googleapis.com/oauth2/v4/token"
    #     self.credentials = Credentials(json.dumps(self.oauth2.token))

    def authenticate(self):
        """Guide user through authentication; create `OAuth2Session` for HTTP requests.

        The user will need to visit a URL and authorize `PittGoogleConsumer` to make
        API calls on their behalf.

        The user must have a Google account that is authorized make API calls
        through the project defined by the `GOOGLE_CLOUD_PROJECT` variable in the
        Django `settings.py` file. Any project can be used.

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
            prompt="select_account",
        )
        print(
            f"Please visit this URL to authorize PittGoogleConsumer:\n\n{authorization_url}\n"
        )
        authorization_response = input(
            "Enter the full URL of the page you are redirected to after authorization:\n"
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
        except NotFound:
            raise

    def create_sql_stmnt(self, parameters):
        """Create the SQL statement and the query parameters job config."""
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
        """Unpack messages in `response`. Run `callback` if present."""
        alerts = []
        for row in query_job.result():
            alert_dict = dict(row)

            if callback is not None:
                alert_dict = callback(alert_dict, **kwargs)

            if alert_dict is not None:
                alerts.append(alert_dict)

        return alerts

    def _log_and_print(self, msg, severity="INFO"):
        # request = {
        #     'logName': self.log_name,
        #     'resource': {
        #         'type': 'pubsub_subscription',
        #         'labels': {
        #             'project_id': settings.GOOGLE_CLOUD_PROJECT,
        #             'subscription_id': self.subscription_name
        #         },
        #     },
        #     'entries': [{'textPayload': msg, 'severity': severity}],
        # }
        # response = self.oauth2.post(self.logging_url, json=json.dumps(request))
        # print(response.content)
        # response.raise_for_status()
        print(msg)
