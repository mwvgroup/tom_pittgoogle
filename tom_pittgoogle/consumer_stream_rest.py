#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to manage Pub/Sub connections via REST, and work with message data.

Pub/Sub REST API docs: https://cloud.google.com/pubsub/docs/reference/rest

Used by `BrokerStreamRest`, but can be called independently.

Basic workflow:

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
   ConsumerStreamRest.touch_subscription
   ConsumerStreamRest.unpack_and_ack_messages

"""

from django.conf import settings
import json
from requests_oauthlib import OAuth2Session

from .utils.templatetags.utility_tags import b64avro_to_dict


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class ConsumerStreamRest:
    """Consumer class to manage Pub/Sub connections and work with messages.

    Initialization does the following:

        - Authenticate the user. Create an `OAuth2Session` object for the user/broker
          to make HTTP requests with.

        - Make sure the subscription exists and we can connect. Create it, if needed.
    """

    def __init__(self, subscription_name):
        """Create an `OAuth2Session`. Set `subscription_url` and check connection."""
        self.authenticate()

        # logger
        # TODO: debug the logger
        # the user probably prefers a more standard logger anyway
        # self.logging_url = "https://logging.googleapis.com/v2/entries:write"
        # self.log_name = (
        #     f"projects/{settings.GOOGLE_CLOUD_PROJECT}/logs/{subscription_name}"
        # )

        # subscription
        self.subscription_name = subscription_name
        self.subscription_path = (
            f"projects/{settings.GOOGLE_CLOUD_PROJECT}/subscriptions/{subscription_name}"
        )
        self.subscription_url = (
            f"https://pubsub.googleapis.com/v1/{self.subscription_path}"
        )
        self.topic_path = ""  # for user info only. set in touch_subscription()
        self.touch_subscription()

    def authenticate(self):
        """Guide user through authentication; create `OAuth2Session` for HTTP requests.

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
        redirect_uri = "https://ardent-cycling-243415.appspot.com/"  # TODO: better page
        scopes = [
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/pubsub",
        ]
        oauth2 = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)

        # instruct the user to authorize
        authorization_url, state = oauth2.authorization_url(
            "https://accounts.google.com/o/oauth2/auth",
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

    def touch_subscription(self):
        """Make sure the subscription exists and we can connect.

        If the subscription doesn't exist, try to create one (in the user's project)
        that is attached to a topic of the same name in the Pitt-Google project.

        Note that messages published before the subscription is created are not
        available.
        """
        # check if subscription exists
        response = self.oauth2.get(self.subscription_url)

        if response.status_code == 200:
            # subscription exists. tell the user which topic it's connected to.
            self.topic_path = json.loads(response.content)["topic"]
            print(f"Subscription exists: {self.subscription_path}")
            print(f"Connected to topic: {self.topic_path}")

        elif response.status_code == 404:
            # subscription doesn't exist. try to create it.
            self._create_subscription()

        else:
            print(response.content)
            response.raise_for_status()

    def _create_subscription(self):
        """Try to create the subscription."""
        topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{self.subscription_name}"
        request = {"topic": topic_path}
        put_response = self.oauth2.put(f"{self.subscription_url}", data=request)

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
        """Unpack and acknowledge messages in `response`; run `callback` if present.

        If `lighten_alerts` is True, drop extra fields and flatten the alert dict.

        `callback` is assumed to be a filter. It should accept an alert dict
        and return the dict if the alert passes the filter, else return None.
        """
        # unpack and run the callback
        try:
            msgs = response.json()["receivedMessages"]
        except KeyError:
            msg = (
                "No messages received. If you recently created the subscription, "
                "it's possible that no messages have been published to the "
                "topic since the subscription's creation."
                "Try connecting to the heartbeat stream ztf-loop."
            )
            raise ValueError(msg)

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
        """Return the alert as a flat dict, keeping only the fields in `keep_fields`."""
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
        """Send message acknowledgements to Pub/Sub."""
        response = self.oauth2.post(
            f"{self.subscription_url}:acknowledge", data={"ackIds": ack_ids},
        )
        response.raise_for_status()

    def delete_subscription(self):
        """Delete the subscription.

        This is provided for the user's convenience, but it is not necessary and is not
        automatically called.

            - Storage of unacknowledged Pub/Sub messages does not result in fees.

            - Unused subscriptions automatically expire; default is 31 days.
        """
        response = self.oauth2.delete(self.subscription_url)
        if response.status_code == 200:
            self._log_and_print(f"Deleted subscription: {self.subscription_path}")
        elif response.status_code == 404:
            print(
                f"Nothing to delete, subscription does not exist: {self.subscription_path}"
            )
        else:
            response.raise_for_status()

    def _log_and_print(self, msg, severity="INFO"):
        # TODO: fix this
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
