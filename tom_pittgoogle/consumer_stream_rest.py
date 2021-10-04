#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to pull alerts from a Pitt-Google message stream.

Connects to the user's Pub/Sub subscription via the REST API.

API Docs: https://cloud.google.com/pubsub/docs/reference/rest
"""

from django.conf import settings
import json
from requests_oauthlib import OAuth2Session

from .utils.templatetags.utility_tags import b64avro_to_dict


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class PittGoogleConsumer:
    """Manage a subscription that is linked to a topic published by Pitt-Google."""

    def __init__(self, subscription_name):
        """Open a subscriber client. If the subscription doesn't exist, create it.

        View logs:
            1. https://console.cloud.google.com
            2.

        Authentication creates an `OAuth2Session` object which can be used to fetch
        data, for example: `response = PittGoogleConsumer.oauth.get({url})`
        """
        self._authenticate()

        # logger
        # TODO: debug the logger. it currently doesn't not work
        self.logging_url = "https://logging.googleapis.com/v2/entries:write"
        self.log_name = (
            f"projects/{settings.GOOGLE_CLOUD_PROJECT}/logs/{subscription_name}"
        )

        # subscription
        self.subscription_name = subscription_name
        self.subscription_path = (
            f"projects/{settings.GOOGLE_CLOUD_PROJECT}/subscriptions/{subscription_name}"
        )
        self.subscription_url = (
            f"https://pubsub.googleapis.com/v1/{self.subscription_path}"
        )
        self.topic_path = ""  # for user info only. set in _get_create_subscription()
        self._get_create_subscription()

    def _authenticate(self):
        """Authenticate the user via OAuth 2.0.

        The user will need to visit a URL and authorize `PittGoogleConsumer` to manage
        resources through their Google account.
        """
        # create an OAuth2Session
        client_id = settings.PITTGOOGLE_OAUTH_CLIENT_ID
        client_secret = settings.PITTGOOGLE_OAUTH_CLIENT_SECRET
        redirect_uri = "https://ardent-cycling-243415.appspot.com/"  # TODO: better page
        scopes = [
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/pubsub",
        ]
        oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)

        # instruct the user to authorize
        authorization_url, state = oauth.authorization_url(
            "https://accounts.google.com/o/oauth2/auth",
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
        token = oauth.fetch_token(
            "https://accounts.google.com/o/oauth2/token",
            authorization_response=authorization_response,
            client_secret=client_secret,
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
            self.topic_path = json.loads(get_response.content)["topic"]
            print(f"Subscription exists: {self.subscription_path}")
            print(f"Connected to topic: {self.topic_path}")

        elif get_response.status_code == 404:
            # subscription doesn't exist. try to create it.
            self._create_subscription()

        else:
            print(get_response.content)
            get_response.raise_for_status()

    def _create_subscription(self):
        """Try to create the subscription."""
        topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{self.subscription_name}"
        request = {"topic": topic_path}
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

    def unpack_and_ack_messages(
        self, response, lighten_alerts=False, callback=None, **kwargs
    ):
        """Unpack and acknowledge messages in `response`. Run `callback` if present."""
        # unpack and run the callback
        msgs = response.json()["receivedMessages"]
        alerts, ack_ids = [], []
        for msg in msgs:
            alert_dict = b64avro_to_dict(msg["message"]["data"])

            if lighten_alerts:
                alert_dict = self._lighten_alert(alert_dict)

            if callback is not None:
                alert_dict = callback(alert_dict, **kwargs)

            if alert_dict is not None:
                alerts.append(alert_dict)
            ack_ids.append(msg["ackId"])

        # acknowledge messages so they leave the subscription
        self._ack_messages(ack_ids)

        return alerts

    def _lighten_alert(self, alert_dict):
        keep_fields = {
            "top-level": ["objectId", "candid", ],
            "candidate": ["jd", "ra", "dec", "magpsf", "classtar", ],
        }
        alert_lite = {k: alert_dict[k] for k in keep_fields["top-level"]}
        alert_lite.update(
            {k: alert_dict["candidate"][k] for k in keep_fields["candidate"]}
        )
        return alert_lite

    def _ack_messages(self, ack_ids):
        response = self.oauth.post(
            f"{self.subscription_url}:acknowledge", data={"ackIds": ack_ids},
        )
        response.raise_for_status()

    def delete_subscription(self):
        """Delete the subscription, if it exists."""
        response = self.oauth.delete(self.subscription_url)
        if response.status_code == 200:
            self._log_and_print(f"Deleted subscription: {self.subscription_path}")
        elif response.status_code == 404:
            print(
                f"Nothing to delete, subscription does not exist: {self.subscription_path}"
            )
        else:
            response.raise_for_status()

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
        # response = self.oauth.post(self.logging_url, json=json.dumps(request))
        # print(response.content)
        # response.raise_for_status()
        print(msg)
