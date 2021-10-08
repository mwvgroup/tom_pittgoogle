#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to pull or query alerts from Pitt-Google.

Pub/Sub Python Client docs: https://googleapis.dev/python/pubsub/latest/index.html
"""

# from concurrent.futures.thread import ThreadPoolExecutor
from django.conf import settings
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
# from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from google.cloud import logging as gc_logging
from google_auth_oauthlib.helpers import credentials_from_session
from requests_oauthlib import OAuth2Session
import queue
from queue import Empty

from .utils.templatetags.utility_tags import avro_to_dict


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
        # the user will be notified and this topic_path will be updated for consistency.
        self.topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{subscription_name}"
        self.get_create_subscription()

        self.database_list = []  # list of dicts. fake database for demo.
        self.queue = queue.Queue()  # queue for communication between threads

        # for the TOM `GenericAlert`. this won't be very helpful without instructions.
        self.pull_url = "https://pubsub.googleapis.com/v1/{subscription_path}"

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

    def stream_alerts(
        self,
        lighten_alerts=False,
        user_filter=None,
        parameters=None,
    ):
        """.
        Args:
            parameters: Must include parameters used in `user_filter`. May also include:
                        max_results:
                        timeout:
                        max_backlog:
        Higher numbers may increase throughput. Lower numbers decrease  Alerts that are never processed are _not_ dropped from the subscription.
        """
        # callback doesn't currently accept kwargs. set attributes instead.
        self.user_filter = user_filter
        self.parameters = {**parameters, 'lighten_alerts': lighten_alerts}

        # avoid pulling down a large number of alerts that don't get processed
        flow_control = pubsub_v1.types.FlowControl(max_messages=parameters['max_backlog'])

        # Google API has a thread scheduler that can run multiple background threads
        # and includes a queue, but I (Troy) haven't gotten it working yet.
        # self.scheduler = ThreadScheduler(ThreadPoolExecutor(max_workers))
        # self.scheduler.schedule(self.callback, lighten_alerts=lighten_alerts)

        # start pulling and processing msgs using the callback, in a background thread
        self.streaming_pull_future = self.client.subscribe(
            self.subscription_path,
            self.callback,
            flow_control=flow_control,
            # scheduler=self.scheduler,
            # await_callbacks_on_shutdown=True,
        )

        # Use the queue to count saved messages and
        # stop when we hit a max_messages or timeout stopping condition.
        num_saved = 0
        while True:
            try:
                num_saved += self.queue.get(block=True, timeout=parameters['timeout'])
            except Empty:
                break
            else:
                self.queue.task_done()
                if parameters['max_results'] & num_saved >= parameters['max_results']:
                    break
        self._stop()

        self._log_and_print(f"Saved {num_saved} messages from {self.subscription_path}")

        return self.database_list

    def _stop(self):
        """Shutdown the streaming pull in the background thread gracefully.

        Implemented as a separate function so the developer can quickly shut it down
        if things get out of control during dev. :)
        """
        self.streaming_pull_future.cancel()  # Trigger the shutdown.
        self.streaming_pull_future.result()  # Block until the shutdown is complete.

    def callback(self, message):
        """Unpack messages in `response`. Run `callback` if present."""
        params = self.parameters

        alert_dict = avro_to_dict(message.data)

        if params['lighten_alerts']:
            alert_dict = self._lighten_alert(alert_dict)

        if self.user_filter is not None:
            alert_dict = self.user_filter(alert_dict, params)

        # save alert
        if alert_dict is not None:
            if params['save_metadata'] == "yes":
                # nest inside the alert so we don't break the broker
                alert_dict['metadata'] = self._extract_metadata(message)
            self.save_alert(alert_dict)
            num_saved = 1
        else:
            num_saved = 0

        # communicate with the main thread
        self.queue.put(num_saved)
        if params['max_results'] is not None:
            # block until main thread acknowledges so we don't pull too many messages
            self.queue.join()  # single background thread => one-in-one-out

        message.ack()

    def save_alert(self, alert):
        """."""
        self.database_list.append(alert)

    def _extract_metadata(self, message):
        # TOM wants to serialize this and has trouble with the dates.
        # just make everything strings for now
        return {
            "message_id": message.message_id,
            "publish_time": str(message.publish_time),
            # attributes includes the originating 'kafka.timestamp'
            "attributes": {k: str(v) for k, v in message.attributes.items()},
        }

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
