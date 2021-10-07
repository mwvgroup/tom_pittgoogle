#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google broker module for TOM Toolkit.

Connects to the user's Pub/Sub subscription via the Python client.

API Docs: https://googleapis.dev/python/pubsub/latest/subscriber/index.html

.. autosummary::
   :nosignatures:

   tom_pittgoogle.broker_stream_rest.FilterAlertsForm
   tom_pittgoogle.broker_stream_rest.PittGoogleBrokerStreamRest
"""
import os

from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_stream_python import ConsumerStreamPython


SUBSCRIPTION_NAME = "ztf-loop"
# Create the subscription if needed, and make sure we can connect to it.
if 'BUILD_IN_RTD' not in os.environ:
    CONSUMER = ConsumerStreamPython(SUBSCRIPTION_NAME)


class FilterAlertsForm(GenericQueryForm):
    """Basic form for filtering alerts.

    Fields:

        max_results (``IntegerField``)

        classtar_threshold (``FloatField``)

        classtar_gt_lt (``ChoiceField``)
    """

    max_results = forms.IntegerField(
        required=True, initial=10, min_value=1
    )
    classtar_threshold = forms.FloatField(
        required=False,
        initial=0.5,
        min_value=0,
        max_value=1,
        help_text="Star/Galaxy score threshold",
    )
    classtar_gt_lt_choices = [("lt", "less than"), ("gt", "greater than or equal")]
    classtar_gt_lt = forms.ChoiceField(
        required=True,
        choices=classtar_gt_lt_choices,
        initial="lt",
        widget=forms.RadioSelect,
        label="",
    )


class BrokerStreamPython(GenericBroker):
    """Pitt-Google broker interface to pull alerts from Pub/Sub via the Python client."""

    name = "Pitt-Google stream python"
    form = FilterAlertsForm

    @classmethod
    def fetch_alerts(self, parameters):
        """Pull or query alerts, unpack, apply the user filter, return an iterator."""
        clean_params = self._clean_parameters(parameters)
        num_msgs = CONSUMER.stream_alerts(
            max_messages=10,
            max_workers=1,
            timeout=2,
            max_msg_backlog=10,
            lighten_alerts=True,
        )
        # alerts = self.request_alerts(clean_params)
        # return iter(alerts)

    def request_alerts(self, parameters):
        """Pull or query alerts, unpack, apply the user filter."""
        response = CONSUMER.oauth.post()
        response.raise_for_status()
        alerts = CONSUMER.unpack_messages(
            response,
            lighten_alerts=True,
            callback=self._user_filter,
            parameters=parameters,
        )
        return alerts

    @staticmethod
    def _user_filter(alert, parameters):
        """Apply the filter indicated by the form's parameters.

        Args:
            `alert_dict`: Single alert, ZTF packet data.
            `parameters`: parameters submitted by the user through the form.
        """
        alert_passes_filter = True
        if alert_passes_filter:
            return alert
        else:
            return None

    def _clean_parameters(self, parameters):
        clean_params = dict(parameters)
        return clean_params

    @classmethod
    def to_generic_alert(self, alert):
        """Map the Pitt-Google alert to a TOM `GenericAlert`."""
        return GenericAlert(
            timestamp=alert["jd"],
            url="https://{service}.googleapis.com/{resource_path}",
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["magpsf"],
            score=alert["classtar"],
        )
