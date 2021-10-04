#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google module for TOM Toolkit. Retrieves alerts from Pub/Sub message streams."""

from django import forms
from django.conf import settings
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker
import requests
from urllib.parse import urlencode

# from tom_alerts.models import BrokerQuery
# from tom_targets.models import Target
# import requests

from .utils.templatetags.utility_tags import avro_to_dict
from pittgoogle_consumer_rest import PittGoogleConsumerRest


SUBSCRIPTION_NAME = "ztf-loop"


class FilterAlertsStreamForm(GenericQueryForm):
    """Form for filtering a Pub/Sub stream.

    Fields:
        objectId (``CharField``)
        candid (``IntegerField``)
        max_results (``IntegerField``)
    """

    # objectId = forms.CharField(required=False)
    # candid = forms.IntegerField(required=False)
    max_results = forms.IntegerField(
        required=True, initial=10, min_value=1, max_value=100
    )
    # timeout = forms.IntegerField(required=True, initial=10, min_value=1, max_value=600)


class PittGoogleBrokerStream(GenericBroker):
    """Pitt-Google broker interface to listen to a Pub/Sub stream."""

    name = "Pitt-Google stream"
    form = FilterAlertsStreamForm

    def __init__(self):
        """Create the subscription if needed, and make sure we can connect to it."""
        self.consumer = PittGoogleConsumerRest(SUBSCRIPTION_NAME)

    def fetch_alerts(self, parameters):
        response = self._request_alerts(parameters)
        alerts = response["receivedMessages"]
        return iter(alerts)

    def _request_alerts(self, parameters):
        """Pull alerts from the subscription, using a POST request.

        https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull
        """
        subscription_path = f"projects/{settings.GOOGLE_CLOUD_PROJECT}/subscriptions/{SUBSCRIPTION_NAME}"
        response = requests.post(
            f"{PITTGOOGLE_URL_STUB}/v1/{subscription_path}:pull",
            data={"maxMessages": parameters["max_results"]},
        )
        response.raise_for_status()
        return response.json()

    def _clean_parameters(parameters):
        clean_params = dict(parameters)
        return clean_params

    @classmethod
    def to_generic_alert(self, alert):
        """."""
        return GenericAlert(
            timestamp=alert["jd"],
            url=query_url,
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["mag"],
            score=alert["classtar"],
        )
