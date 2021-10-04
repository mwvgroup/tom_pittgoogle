#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Class to listen to Pitt-Google's Pub/Sub streams in a Django environment."""

from django.conf import settings
from google_auth_oauthlib import flow
from google.api_core.exceptions import NotFound
import json

# from google.oauth2 import service_account
# from google.cloud import logging, pubsub_v1
# import requests
from requests_oauthlib import OAuth2Session


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class PittGoogleConsumerRest:
    """Manage a Pub/Sub subscription that is linked to a topic published by Pitt-Google."""

    def __init__(self, subscription_name):
        """Open a subscriber client. If the subscription doesn't exist, create it.

        View logs:
            1. https://console.cloud.google.com
            2.
        """
        # user credentials
        # self.credentials = service_account.Credentials.from_service_account_file(
        #     settings.GOOGLE_APPLICATION_CREDENTIALS
        # )
        self._authenticate()

        # logger
        self.logging_url = "https://logging.googleapis.com/v2/entries:write"
        self.log_name = f"projects/{settings.GOOGLE_CLOUD_PROJECT}/logs/{subscription_name}"
        # self.logger = logging.Client(credentials=self.credentials).logger(log_name)

        # subscriber client
        # self.subscriber = pubsub_v1.SubscriberClient(credentials=self.credentials)

        # subscription
        self.subscription_name = subscription_name
        self.subscription_path = f"projects/{settings.GOOGLE_CLOUD_PROJECT}/subscriptions/{subscription_name}"
        # self.topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{self.subscription_name}"
        self.topic_path = ""  # only for user info. set in _get_create_subscription()
        self.subscription_url = f'https://pubsub.googleapis.com/v1/{self.subscription_path}'

        self._get_create_subscription()

    # def unpack_and_ack(self, )

    def _authenticate(self):
        # client_secrets = '/Users/troyraen/Documents/broker/repo/GCP_oauth-mypgb-raentroy.json'
        # appflow = flow.InstalledAppFlow.from_client_secrets_file(
        #     client_secrets, scopes=["https://www.googleapis.com/auth/pubsub"]
        # )
        # appflow.run_local_server()
        # credentials = appflow.credentials
        client_id = settings.PITTGOOGLE_OAUTH_CLIENT_ID
        client_secret = settings.PITTGOOGLE_OAUTH_CLIENT_SECRET
        # redirect_uri = "http://localhost:8080/"
        redirect_uri = "https://ardent-cycling-243415.appspot.com/"  # TODO: better page
        scopes = [
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/pubsub",
        ]
        oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)
        authorization_url, state = oauth.authorization_url(
            'https://accounts.google.com/o/oauth2/auth',
            access_type="offline",
            prompt="select_account"
        )
        print(f'Please visit this URL to authorize PittGoogleConsumer: {authorization_url}')
        authorization_response = input('Enter the full callback URL\n')
        _ = oauth.fetch_token(
            'https://accounts.google.com/o/oauth2/token',
            authorization_response=authorization_response,
            client_secret=client_secret
        )
        self.oauth = oauth

    def _get_create_subscription(self):
        """Create the subscription if needed.

        1.  Check whether a subscription exists in the user's GCP project.

        2.  If it doesn't, try to create a subscription in the user's project that is
            attached to a topic of the same name in the Pitt-Google project.
        """
        # check if subscription exists
        get_response = self.oauth.get(self.subscription_url)

        if get_response.status_code == 200:
            # subscription exists. tell the user which topic it's connected to.
            self.topic_path = json.loads(get_response.content)['topic']
            print(f"Subscription exists: {self.subscription_path}")
            print(f"Connected to topic: {self.topic_path}")

        elif get_response.status_code == 404:
            # subscription doesn't exist. try to create it.
            topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{self.subscription_name}"
            request = {'topic': topic_path}
            put_response = self.oauth.put(f"{self.subscription_url}", data=request)

            if put_response.status_code == 200:
                # subscription created successfully
                self.topic_path = topic_path
                self._log_and_print(
                    (
                        f"Created subscription: {self.subscription_path}\n"
                        f"Connected to topic: {self.topic_path}"
                    )
                )

            elif put_response.status_code == 404:
                raise ValueError(
                    (
                        f"A subscription named {self.subscription_name} does not exist"
                        "in the Google Cloud Platform project "
                        f"{settings.GOOGLE_CLOUD_PROJECT}, "
                        "and one cannot be create because Pitt-Google does not "
                        "publish a public topic with the same name."
                    )
                )

            else:
                # if the subscription name is invalid, content has helpful info
                print(put_response.content)
                put_response.raise_for_status()

        else:
            print(get_response.content)
            get_response.raise_for_status()

    def delete_subscription(self):
        """Delete the subscription, if it exists."""
        response = self.oauth.delete(self.subscription_url)
        if response.status_code == 200:
            self._log_and_print(f"Deleted subscription: {self.subscription_path}")
        elif response.status_code == 404:
            print(f"Nothing to delete, subscription does not exist: {self.subscription_path}")
        else:
            response.raise_for_status()

    def _log_and_print(self, msg, severity="INFO"):
        print(msg)
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
        # response = self.oauth.post(self.logging_url, json=json.dumps(request))
        # print(response.content)
        # response.raise_for_status()
