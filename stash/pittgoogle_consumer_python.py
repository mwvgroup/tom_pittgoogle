#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Class to listen to Pitt-Google's Pub/Sub streams in a Django environment."""

from django.conf import settings
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from google.cloud import logging, pubsub_v1


PITTGOOGLE_PROJECT_ID = "ardent-cycling-243415"


class PittGoogleConsumerPython:
    """Manage a Pub/Sub subscription that is linked to a topic published by Pitt-Google."""

    def __init__(self, subscription_name):
        """Open a subscriber client. If the subscription doesn't exist, create it.

        View logs:
            1. https://console.cloud.google.com
            2.
        """
        # user credentials
        self.credentials = service_account.Credentials.from_service_account_file(
            settings.GOOGLE_APPLICATION_CREDENTIALS
        )

        # logger
        log_name = subscription_name
        self.logger = logging.Client(credentials=self.credentials).logger(log_name)

        # subscriber client
        self.subscriber = pubsub_v1.SubscriberClient(credentials=self.credentials)

        # subscription
        self.subscription_name = subscription_name
        # self.subscription_path = self.subscriber.subscription_path(
        #     settings.GOOGLE_CLOUD_PROJECT, self.subscription_name
        # )
        self.subscription_path = f"projects/{settings.GOOGLE_CLOUD_PROJECT}/subscriptions/{subscription_name}"
        self.topic_path = f"projects/{PITTGOOGLE_PROJECT_ID}/topics/{subscription_name}"
        self.create_subscription()

    # def unpack_and_ack(self, )

    def create_subscription(self):
        """Create the subscription if needed.

        1.  Check whether a subscription exists in the user's GCP project.

        2.  If it doesn't, try to create a subscription in the user's project that is
            attached to a topic of the same name in the Pitt-Google project.
        """
        try:
            # check if subscription exists
            self.subscriber.get_subscription(subscription=self.subscription_path)

        except NotFound:
            # subscription does not exist. try to create it.
            # publisher = pubsub_v1.PublisherClient(credentials=self.credentials)
            # topic_path = publisher.topic_path(
            #     PITTGOOGLE_PROJECT_ID, self.subscription_name
            # )
            try:
                self.subscriber.create_subscription(
                    name=self.subscription_path, topic=self.topic_path
                )
            except NotFound:
                # suitable topic does not exist in the Pitt-Google project
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
                self._log_and_print(f"Created subscription: {self.subscription_path}")

        else:
            print(f"Subscription exists: {self.subscription_path}")

    def delete_subscription(self):
        """Delete the subscription, if it exists."""
        try:
            # check if subscription exists
            self.subscriber.get_subscription(subscription=self.subscription_path)
        except NotFound:
            print(f"Nothing to delete, subscription does not exist: {self.subscription_path}")
        else:
            self.subscriber.delete_subscription(subscription=self.subscription_path)
            self._log_and_print(f"Deleted subscription: {self.subscription_path}")

    def _log_and_print(self, msg, severity="INFO"):
        print(msg)
        self.logger.log_text(msg, severity=severity)


class PittGoogleRESTSubscriber:
    """Manage a Pub/Sub subscription that is linked to a topic published by Pitt-Google."""

    def __init__(self, subscription_name):
        """Open a subscriber client. If the subscription doesn't exist, create it.

        View logs:
            1. https://console.cloud.google.com
            2.
        """
        # user credentials
        self.credentials = service_account.Credentials.from_service_account_file(
            settings.GOOGLE_APPLICATION_CREDENTIALS
        )

        # logger
        log_name = subscription_name
        self.logger = logging.Client(credentials=self.credentials).logger(log_name)

        # subscriber client
        self.subscriber = pubsub_v1.SubscriberClient(credentials=self.credentials)

        # subscription
        self.subscription_name = subscription_name
        self.subscription_path = self.subscriber.subscription_path(
            settings.GOOGLE_CLOUD_PROJECT, self.subscription_name
        )  # projects/{GOOGLE_CLOUD_PROJECT}/subscriptions/{subscription_name}
        self.create_subscription()

    # def unpack_and_ack(self, )

    def create_subscription(self):
        """Create the subscription if needed.

        1.  Check whether a subscription exists in the user's GCP project.

        2.  If it doesn't, try to create a subscription in the user's project that is
            attached to a topic of the same name in the Pitt-Google project.
        """
        try:
            # check if subscription exists
            self.subscriber.get_subscription(subscription=self.subscription_path)

        except NotFound:
            # subscription does not exist. try to create it.
            publisher = pubsub_v1.PublisherClient(credentials=self.credentials)
            topic_path = publisher.topic_path(
                settings.GOOGLE_CLOUD_PROJECT, self.subscription_name
            )
            try:
                self.subscriber.create_subscription(
                    name=self.subscription_path, topic=topic_path
                )
            except NotFound:
                # suitable topic does not exist in the Pitt-Google project
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
                self._log_and_print(f"Created subscription: {self.subscription_path}")

        else:
            print(f"Subscription exists: {self.subscription_path}")

    def delete_subscription(self):
        """Delete the subscription, if it exists."""
        try:
            # check if subscription exists
            self.subscriber.get_subscription(subscription=self.subscription_path)
        except NotFound:
            print(f"Nothing to delete, subscription does not exist: {self.subscription_path}")
        else:
            self.subscriber.delete_subscription(subscription=self.subscription_path)
            self._log_and_print(f"Deleted subscription: {self.subscription_path}")

    def _log_and_print(self, msg, severity="INFO"):
        print(msg)
        self.logger.log_text(msg, severity=severity)
