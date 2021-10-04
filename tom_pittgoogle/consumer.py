#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to pull or query alerts from Pitt-Google."""

from django.conf import settings
from requests_oauthlib import OAuth2Session

from .utils.templatetags.utility_tags import b64avro_to_dict


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class PittGoogleConsumer:
    """Consumer class to pull or query alerts from Pitt-Google, and manipulate them."""

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
        # TODO: add needed params/logic

        # subscription or table resource
        # TODO: add needed params/logic
        self._get_resource()

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
        ]
        oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)

        # instruct the user to authorize
        authorization_url, state = oauth.authorization_url(
            "https://accounts.google.com/o/oauth2/auth",
            access_type="offline",
            prompt="select_account",
        )
        print(
            f"Please visit this URL to authorize PittGoogleConsumer:\n{authorization_url}"
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

    def _get_resource(self):
        """Make sure the resource exists, and we can connect to it."""
        # TODO: add needed logic
        return

    def unpack_messages(
        self, response, lighten_alerts=False, callback=None, **kwargs
    ):
        """Unpack messages in `response`. Run `callback` if present."""
        msgs = response.json()["receivedMessages"]
        alerts = []
        for msg in msgs:
            alert_dict = b64avro_to_dict(msg["message"]["data"])

            if lighten_alerts:
                alert_dict = self._lighten_alert(alert_dict)

            if callback is not None:
                alert_dict = callback(alert_dict, **kwargs)

            if alert_dict is not None:
                alerts.append(alert_dict)

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
