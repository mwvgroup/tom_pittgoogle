#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Consumer class to pull Pub/Sub messages via a Python client, and work with data.

Pub/Sub Python Client docs: https://googleapis.dev/python/pubsub/latest/index.html

Used by `BrokerStreamPython`, but can be called independently.

Use-case: Save alerts to a database

    The demo for this implementation assumes that the real use-case is to save alerts
    to a database rather than view them through a TOM site.
    Therefore, the `Consumer` currently saves the alerts in real time,
    and then simply returns a list of alerts after all messages are processed.
    That list is then coerced into an iterator by the `Broker`.
    If the user really cares about the `Broker`'s iterator, `stream_alerts` can
    be tweaked to yield the alerts in real time.

Basic workflow:

.. code:: python

    consumer = ConsumerStreamPython(subscription_name)

    alert_dicts_list = consumer.stream_alerts(
        user_callback=user_callback,
        **user_kwargs,
    )
    # alerts are processed and saved in real time. the list is returned for convenience.

See especially:

.. autosummary::
   :nosignatures:

   ConsumerStreamPython.stream_alerts
   ConsumerStreamPython.callback

"""

import logging

# from concurrent.futures.thread import ThreadPoolExecutor
from django.conf import settings
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
# from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from google import auth
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google_auth_oauthlib.helpers import credentials_from_session
from requests_oauthlib import OAuth2Session
import queue
from queue import Empty

from .utils.templatetags.utility_tags import avro_to_dict


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class ConsumerStreamPython:
    """Consumer class to manage Pub/Sub connections and work with messages.

    Initialization does the following:

        - Authenticate the user via a key file or OAuth 2.0.

        - Add a Google Cloud logging handler to the LOGGER. To view,
          visit: https://console.cloud.google.com/logs.

        - Create a client object (`google.cloud.pubsub_v1.SubscriberClient`).

        - Create a `queue.Queue` object to communicate with the background thread
          running the streaming pull.

        - Make sure the subscription exists and we can connect. Create it, if needed.
    """

    def __init__(self, subscription_name, save_fields=None):
        """Authenticate user; create client; set subscription path; check connection."""
        user_project = settings.GOOGLE_CLOUD_PROJECT
        self.database_list = []  # list of dicts. fake database for demo.

        LOGGER.info((
            f"Instantiating a consumer for project: {user_project}, "
            f"subscription: {subscription_name}"
        ))

        # authenticate the user
        self.get_credentials(user_project)

        # logger
        gclogging_client = google.cloud.logging.Client(credentials=self.credentials)
        handler = CloudLoggingHandler(gclogging_client, name=__name__)
        LOGGER.addHandler(handler)

        # subscriber client
        self.client = pubsub_v1.SubscriberClient(credentials=self.credentials)

        # subscription
        self.subscription_name = subscription_name
        self.subscription_path = f"projects/{user_project}/subscriptions/{subscription_name}"
        # Topic to connect the subscription to, if it needs to be created.
        # If the subscription already exists but is connected to a different topic,
        # the user will be notified and this topic_path will be updated for consistency.
        self.topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{subscription_name}"
        self.touch_subscription()

        # field names to keep in the unpacked alert_dict
        self._set_save_fields(save_fields)

        # queue for communication between threads. Enforces stopping conditions.
        self.queue = queue.Queue()

        # for the TOM `GenericAlert`. this won't be very helpful without instructions.
        self.pull_url = "https://pubsub.googleapis.com/v1/{subscription_path}"

    def stream_alerts(
        self,
        send_data="alert_dict",
        include_metadata=False,
        user_callback=None,
        return_list=False,
        flow_configs=None,
        **user_kwargs
    ):
        """Execute a streaming pull, processing messages through `self.callback`.

        The streaming pull happens in a background thread. A `queue.Queue` is used
        to communicate between threads and enforce the stopping condition(s).

        Args:
            send_data (str):    String specifying the data and format requested.
                                Options are:
                                'alert_dict' (default, alert packet data
                                cast to a dictionary),
                                'alert_bytes' (alert packet data
                                as bytes, as received from Pub/Sub),
                                'full_msg'
                                (complete `google.pubsub_v1.types.PubsubMessage` object)

            include_metadata (bool):
                                If False (default), message will be returned acccording
                                to `send_data`.
                                If True, message will be delivered as a dictionary with
                                two keys: 'metadata_dict' and one of 'alert_dict' or
                                'alert_bytes' according to `send_data`.
                                This has no effect if format='full_msg'.

            user_callback (Callable):   Used by `self.callback` to perform user-
                                        requested processing in the background thread.
                                        It must accept the message as the first argument
                                        (type and contents determined by `send_data` and
                                        `include_metadata`),
                                        and can accept arbitrary keyword arguments
                                        which should be passed via `user_kwargs`.
                                        It should return a dictionary with two keys,
                                        'ack' and 'result', with values as follows.
                                        'ack': boolean indicating whether
                                        the message should be acknowledged to Pub/Sub.
                                        Use False to indicate that the message cannot
                                        be processed at this time, and Pub/Sub should
                                        redeliver it in the future.
                                        'result': (arbitrary type) the result of the
                                        processing. If `return_list` is True, this is
                                        the value that will be returned in the list.
                                        If this is explicitly set to None,
                                        the message will not be counted towards
                                        `max_results`, and it will not be returned in
                                        the list (if `return_list` is True).
                                        If the 'result' key is absent, the
                                        message will be counted towards `max_results`,
                                        and the original message data will be returned
                                        in the list (if `return_list` is True).

            return_list (bool): If True, messages will be processed through
                                `self.callback`, and results will be collected in
                                memory and returned as a list after the streaming pull
                                is complete.
                                If False (default), the `user_callback` is responsible
                                for saving the processing results.

            flow_configs (dict):    Dictionary defining stopping conditions and flow
                                    control settings for the streaming pull.
                                    May contain keys:
                                    `max_results` (int, default=10, use None for no
                                    limit; maximum number of messages to process/collect
                                    before stopping)
                                    `timeout` (int; default=30, use None for no
                                    limit; maximum number of seconds to wait for a
                                    new message before stopping)
                                    `max_backlog` (int, default=min(1000, `max_results`;
                                    maximum number of received but unprocessed messages
                                    before pausing the streaming pull).

            user_kwargs (dict): Dict defining keyword arguments to be sent to
                                `user_callback`.

        """
        flow_configs = self._clean_flow_configs(flow_configs)
        # callback doesn't accept kwargs. set attribute instead.
        self.callback_kwargs = {
            "send_data": send_data,
            "include_metadata": include_metadata,
            "user_callback": user_callback,
            "return_list": return_list,
            "flow_configs": flow_configs,
            **user_kwargs,
        }

        # avoid pulling down a large number of alerts that don't get processed
        flow_control = pubsub_v1.types.FlowControl(
            max_messages=flow_configs['max_backlog']
        )

        # multi-threading
        # Google API has a thread scheduler that can run multiple background threads
        # and includes a queue, but I (Troy) haven't gotten it working yet.
        # self.scheduler = ThreadScheduler(ThreadPoolExecutor(max_workers))
        # self.scheduler.schedule(self.callback, lighten_alerts=lighten_alerts)

        # pull and process msgs using the callback, in a background thread
        LOGGER.info(f"Starting the streaming pull with configs {self.callback_kwargs}")
        self.streaming_pull_future = self.client.subscribe(
            self.subscription_path,
            self.callback,
            flow_control=flow_control,
            # scheduler=self.scheduler,
            # await_callbacks_on_shutdown=True,
        )

        try:
            # Use the queue to count saved messages and
            # stop when we hit a max_results or timeout stopping condition.
            num_saved = 0
            while True:
                try:
                    num_saved += self.queue.get(block=True, timeout=flow_configs['timeout'])
                except Empty:
                    break
                else:
                    self.queue.task_done()
                    if flow_configs['max_results'] & num_saved >= flow_configs['max_results']:
                        break
            self._stop()

        # We must catch all errors so we can
        # stop the streaming pull and close the background thread
        # before raising them.
        except (KeyboardInterrupt, Exception):
            self._stop()
            raise

        LOGGER.info(
            (
                f"Saved {num_saved} messages from {self.subscription_path}. "
                "The actual number of messages pulled from Pub/Sub and acknowledged "
                "may be higher, if the user elected not to count some messages "
                "by explicitly returning result=None from the user_callback."
            )
        )

        if return_list:
            return self.database_list

    def callback(self, message):
        """Process a single alert; run user filter; save alert; acknowledge Pub/Sub msg.

        Used as the callback for the streaming pull.
        """
        kwargs = self.callback_kwargs

        # Unpack the message
        try:
            if kwargs["send_data"] == "full_msg":
                msg_data = message
            else:
                msg_data = self._unpack(
                    message, kwargs["send_data"], kwargs["send_metadata"]
                )
        # Catch any errors that weren't explicitly caught during unpacking.
        except Exception as e:
            # log the error, nack the message, and exit the callback.
            LOGGER.warning(
                (
                    "Error unpacking message; it will not be processed or acknowledged."
                    f"\nMessage: {message}"
                    f"\nError: {e}"
                )
            )
            message.nack()  # nack so message does not leave subscription
            return

        # Run the user_callback
        if kwargs["user_callback"] is not None:
            try:
                response = kwargs["user_callback"](msg_data, **kwargs)  # dict

            # catch any errors that weren't explicitly caught in the user_callback.
            except Exception as e:
                ack = False
                log_msg = (
                    "Error running user_callback. "
                    f"Message will not be acknowledged. {e}"
                )

            else:
                # check whether we should ack
                ack = response["ack"]
                if not ack:
                    log_msg = (
                        "user_callback requested to nack (not acknowledge) the message."
                    )

            finally:
                if not ack:
                    # log the log_msg, nack the message, and return
                    LOGGER.warning(log_msg)
                    message.nack()  # nack = not acknowledge
                    return

                else:
                    # grab the data to be collected
                    result_data = response.get("result", msg_data)

        # No user_callback was supplied; just grab the data
        else:
            result_data = msg_data
            ack = True

        # Count results (for max_results stopping condition). Collect them, if requested
        if result_data is not None:
            count = 1
            if kwargs["return_list"]:
                self._collect_data(result_data)
        else:
            count = 0

        # Communicate with the main thread
        self.queue.put(count)
        if kwargs['max_results'] is not None:
            # block until main thread acknowledges so we don't ack msgs that get lost
            self.queue.join()  # single background thread => one-in-one-out

        # Acknowledge the message. If we get here, ack should be True, but check anyway.
        if ack:
            message.ack()

    def _unpack(self, message, send_data, send_metadata):
        """Unpack message data and return according to request.

        Args:
            message (google.pubsub_v1.types.PubsubMessage): Pub/Sub message
            send_data (str):    Format for returned message data. Must be one of
                                'alert_bytes' or 'alert_dict'.
            send_metadata (bool):   Whether to attach the message metadata.

        Returns:
            If `send_metadata` is True, returns a dict with two keys:
            'metadata_dict' and `send_data` (either 'alert_bytes' or 'alert_dict').
            Else returns the message data only, in the format specified by `send_data`.
        """
        if send_data == "alert_bytes":
            alert = message.data
        elif send_data == "alert_dict":
            alert = avro_to_dict(message.data)
            # If the topic is ztf-loop, alert is a nested dict with ZTF schema.
            # Let's turn this into something closer to streams after Issue #101
            # https://github.com/mwvgroup/Pitt-Google-Broker/issues/101
            # Flatten dict and keep fields in self.save_fields, except metadata.
            if self.topic_path.split("/")[-1] == "ztf-loop":
                alert = self._lighten_alert(alert)

        # attach metadata, if requested
        if send_metadata:
            metadata_dict = self._extract_metadata(message)
            msg_data = {send_data: alert, "metadata_dict": metadata_dict}
        else:
            msg_data = alert

        return msg_data

    def _collect_data(self, alert):
        """Save the alert to a database."""
        self.database_list.append(alert)  # fake database for demo

    def _set_save_fields(self, fields=None):
        """Fields to save in the `_unpack` method."""
        if fields is not None:
            self.save_fields = fields
        else:
            self.save_fields = {
                "top-level": ["objectId", "candid", ],
                "candidate": ["jd", "ra", "dec", "magpsf", "classtar", ],
                "metadata": ["message_id", "publish_time", "kafka.timestamp"]
            }

    def _extract_metadata(self, message):
        """Return dict of message metadata requested in self.save_fields['metadata']."""
        # message "attributes" may include Kafka attributes from originating stream:
        # kafka.offset, kafka.partition, kafka.timestamp, kafka.topic
        attributes = {k: v for k, v in message.attributes.items()}
        metadata = {
            "message_id": message.message_id,
            "publish_time": str(message.publish_time),
            **attributes,
        }
        return {k: v for k, v in metadata.items() if k in self.save_fields["metadata"]}

    def _lighten_alert(self, alert_dict):
        alert_lite = {k: alert_dict[k] for k in self.save_fields["top-level"]}
        alert_lite.update(
            {k: alert_dict["candidate"][k] for k in self.save_fields["candidate"]}
        )
        return alert_lite

    def _stop(self):
        """Shutdown the streaming pull in the background thread gracefully."""
        self.streaming_pull_future.cancel()  # Trigger the shutdown.
        self.streaming_pull_future.result()  # Block until the shutdown is complete.

    def _clean_flow_configs(self, flow_configs):
        """Return flow_configs, with defaults inserted for missing key/value pairs.

        Raise an error if the user supplied inappropriate values.
        """
        defaults = {
            "max_results": 10,
            "timeout": 30,  # seconds
            "max_backlog": 1000,
        }

        # create dict of clean configs
        clean_flow_configs = {
            key: self._clean_flow_config_posint_or_none(key, flow_configs, defaults)
            for key in defaults.keys()
        }

        # max_backlog cannot be None
        if clean_flow_configs["max_backlog"] is None:
            maxb = defaults["max_backlog"]
            clean_flow_configs["max_backlog"] = maxb
            LOGGER.warning(f"max_backlog cannot be None. Using default of {maxb}.")
        # be conservative, make sure max_backlog <= max_results
        if (clean_flow_configs["max_results"] is not None) and (
            clean_flow_configs["max_backlog"] > clean_flow_configs["max_results"]
        ):
            clean_flow_configs["max_backlog"] = clean_flow_configs["max_results"]
            LOGGER.warning(
                (
                    "Setting max_backlog = max_results "
                    f"(= {clean_flow_configs['max_results']}) "
                    "to avoid pulling a lot of messages that will not be processed."
                )
            )

        return clean_flow_configs

    @staticmethod
    def _clean_flow_config_posint_or_none(key, flow_configs, defaults):
        """Return value of flow_configs[key] if it exists, else defaults[key].

        Raise an error if the value is not a positive integer or None.
        """
        try:
            result = flow_configs[key]
        except KeyError:
            result = defaults[key]

        # validate that result type is positive integer or None
        finally:
            correct_type = (result is None) or (
                (type(result) == int) and (result > 0)
            )
            if not correct_type:
                msg = f"{key} must be a positive integer or None."
                raise ValueError(msg)

        return result

    def get_credentials(self, user_project):
        """Create user credentials object from service account credentials or an OAuth.

        Try service account credentials first. Fall back to OAuth.
        """
        try:
            # try service account credentials
            self.credentials, project = auth.load_credentials_from_file(
                settings.GOOGLE_APPLICATION_CREDENTIALS
            )
            assert project == user_project  # TODO: handle this better

        except (TypeError, auth.exceptions.DefaultCredentialsError) as ekeyfile:
            # try OAuth2
            try:
                self.authenticate_with_oauth()
                self.credentials = credentials_from_session(self.oauth2)

            except Exception as eoauth:
                msg = f"Cannot load credentials {ekeyfile}, {eoauth}"
                raise PermissionError(msg)

    def authenticate_with_oauth(self):
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

    def touch_subscription(self):
        """Make sure the subscription exists and we can connect.

        If the subscription doesn't exist, try to create one (in the user's project)
        that is attached to a topic of the same name in the Pitt-Google project.

        Note that messages published before the subscription is created are not
        available.
        """
        try:
            # check if subscription exists
            sub = self.client.get_subscription(subscription=self.subscription_path)

        except NotFound:
            self._create_subscription()

        else:
            self.topic_path = sub.topic
            msg = (
                f"Subscription exists: {self.subscription_path}\n"
                f"Connected to topic: {self.topic_path}"
            )
            LOGGER.info(msg)

    def _create_subscription(self):
        """Try to create the subscription."""
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
            msg = (
                f"Created subscription: {self.subscription_path}\n"
                f"Connected to topic: {self.topic_path}"
            )
            LOGGER.info(msg)

    def delete_subscription(self):
        """Delete the subscription.

        This is provided for the user's convenience, but it is not necessary and is not
        automatically called.

            - Storage of unacknowledged Pub/Sub messages does not result in fees.

            - Unused subscriptions automatically expire; default is 31 days.
        """
        try:
            self.client.delete_subscription(subscription=self.subscription_path)
        except NotFound:
            pass
        else:
            LOGGER.info(f'Deleted subscription: {self.subscription_path}')
