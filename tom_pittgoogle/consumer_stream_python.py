#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to pull or query alerts from Pitt-Google."""

from concurrent.futures.thread import ThreadPoolExecutor
# from django.conf import settings
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from google.cloud import logging as gc_logging
from google_auth_oauthlib.helpers import credentials_from_session
from requests_oauthlib import OAuth2Session
import queue
from queue import Empty

import os
import troy_fncs as tfncs
settings = tfncs.AttributeDict({
    'GOOGLE_CLOUD_PROJECT': os.getenv('GOOGLE_CLOUD_PROJECT'),
    'PITTGOOGLE_OAUTH_CLIENT_ID': '591409139500-hb4506vjuao7nvq40k509n7lljf3o3oo.apps.googleusercontent.com',
    'PITTGOOGLE_OAUTH_CLIENT_SECRET': "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",
})
from utils.templatetags.utility_tags import avro_to_dict


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class ConsumerStreamPython:
    """Consumer class to pull or query alerts from Pitt-Google, and manipulate them."""

    def __init__(self, subscription_name):
        """Open a subscriber client. If the subscription doesn't exist, create it."""
        user_project = settings.GOOGLE_CLOUD_PROJECT

        self.authenticate()
        self.credentials = credentials_from_session(self.oauth2)
        self.client = pubsub_v1.SubscriberClient(credentials=self.credentials)

        # logger
        log_client = gc_logging.Client(
            project=user_project, credentials=self.credentials
        )
        self.logger = log_client.logger(subscription_name)

        # subscription
        self.subscription_name = subscription_name
        self.subscription_path = f"projects/{user_project}/subscriptions/{subscription_name}"
        # Topic to connect the subscription to, if it needs to be created.
        # If the subscription already exists but is connected to a different topic,
        # the user will be notified and this value will be changed for consistency.
        self.topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{subscription_name}"
        self.get_create_subscription()

        self.oids = []
        self.q = queue.Queue()

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
            "https://www.googleapis.com/auth/pubsub",
        ]
        oauth2 = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)

        # instruct the user to authorize
        authorization_url, state = oauth2.authorization_url(
            authorization_base_url,
            access_type="offline",
            # prompt="select_account",
            # access_type="online",
            # prompt="auto",
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

    def get_create_subscription(self):
        """Create the subscription if needed.

        1.  Check whether a subscription exists in the user's GCP project.

        2.  If it doesn't, try to create a subscription in the user's project that is
            attached to a topic of the same name in the Pitt-Google project.
        """
        try:
            # check if subscription exists
            sub = self.client.get_subscription(subscription=self.subscription_path)

        except NotFound:
            # subscription does not exist. try to create it.
            try:
                self.client.create_subscription(
                    name=self.subscription_path, topic=self.topic_path
                )
            except NotFound:
                # suitable topic does not exist in the Pitt-Google project
                raise ValueError(
                    (
                        f"A subscription named {self.subscription_name} does not exist"
                        "in the Google Cloud Platform project "
                        f"{settings.GOOGLE_CLOUD_PROJECT}, "
                        "and one cannot be automatically create because Pitt-Google "
                        "does not publish a public topic with the same name."
                    )
                )
            else:
                self._log_and_print(f"Created subscription: {self.subscription_path}")

        else:
            self.topic_path = sub.topic
            print(f"Subscription exists: {self.subscription_path}")
            print(f"Connected to topic: {self.topic_path}")

    def stream_alerts(
        self,
        max_messages=10,
        max_workers=5,
        timeout=10,
        max_msg_backlog=100,
        lighten_alerts=False,
        callback=None,
        **kwargs
    ):
        """."""
        self.oids = []

        # flow_control = pubsub_v1.types.FlowControl(max_messages=max_msg_backlog)
        # self.scheduler = ThreadScheduler(ThreadPoolExecutor(max_workers))
        # self.scheduler = ThreadScheduler()
        # self.scheduler.schedule(self.callback, lighten_alerts=lighten_alerts)
        # self.scheduler.queue.maxsize = 1

        self.streaming_pull_future = self.client.subscribe(
            self.subscription_path,
            self.callback,
            # flow_control=flow_control,
            # scheduler=self.scheduler,
            # await_callbacks_on_shutdown=True,
        )

        # count messages processed until we hit the limit or the timeout
        num_msgs = 0
        while True:
            try:
                # num_msgs += self.scheduler.queue.get(block=True, timeout=timeout)
                # self.scheduler.queue.task_done()
                num_msgs += self.q.get(block=True, timeout=timeout)
                self.q.task_done()
            except Empty:
                break
            else:
                if num_msgs >= max_messages:
                    break

        self.stop()

        self._log_and_print(f"Pulled {num_msgs} messages from {self.subscription_path}")
        return num_msgs

    def stop(self):
        """."""
        self.streaming_pull_future.cancel()  # Trigger the shutdown.
        self.streaming_pull_future.result()  # Block until the shutdown is complete.
        # self.undelivered = self.scheduler.shutdown(await_msg_callbacks=True)
        # self.undelivered = self.scheduler.shutdown()

    def callback(
        self,
        message,
        lighten_alerts=False,
        # user_callback=None,
        # **kwargs
    ):
        """Unpack messages in `response`. Run `callback` if present."""
        alert_dict = avro_to_dict(message.data)
        #
        if lighten_alerts:
            alert_dict = self._lighten_alert(alert_dict)

        # if user_callback is not None:
        #     alert_dict = user_callback(alert_dict, **kwargs)

        self.oids.append(alert_dict['objectId'])

        # tell the main thread that 1 message was processed
        # self.scheduler.queue.put(1, block=True)
        # self.scheduler.queue.join()  # one-in-one-out
        self.q.put(1, block=True)
        self.q.join()  # one-in-one-out

        message.ack()

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

    def delete_subscription(self):
        """Delete the subscription.

        This is provided for user's convenience, but it is not necessary and is not
        automatically called.

            Storage of unacknowledged Pub/Sub messages does not result in fees.

            Unused subscriptions automatically expire; default is 31 days.
        """
        try:
            self.client.delete_subscription(subscription=self.subscription_path)
        except NotFound:
            pass
        else:
            self._log_and_print(f'Deleted subscription: {self.subscription_path}')

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
