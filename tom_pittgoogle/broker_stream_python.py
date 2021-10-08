#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google broker module for TOM Toolkit.

Connects to the user's Pub/Sub subscription via the Python client.

API Docs: https://googleapis.dev/python/pubsub/latest/subscriber/index.html

.. autosummary::
   :nosignatures:

   tom_pittgoogle.broker_stream_rest.FilterAlertsForm
   tom_pittgoogle.broker_stream_rest.BrokerStreamPython
"""
import os

from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_stream_python import ConsumerStreamPython
from .utils.templatetags.utility_tags import jd_to_readable_date


SUBSCRIPTION_NAME = "ztf-loop"
# Create the subscription if needed, and make sure we can connect to it.
if 'BUILD_IN_RTD' not in os.environ:
    CONSUMER = ConsumerStreamPython(SUBSCRIPTION_NAME)


class FilterAlertsForm(GenericQueryForm):
    """Basic form for filtering alerts.

    Fields:

        classtar_threshold (``FloatField``)

        classtar_gt_lt (``ChoiceField``)

        max_results (``IntegerField``)

        timeout (``IntegerField``)

        max_backlog (``IntegerField``)
    """

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
    max_results = forms.IntegerField(
        required=False,
        initial=100,
        min_value=1,
        help_text="Maximum number of alerts to pull and process before stopping the streaming pull."
    )
    timeout = forms.IntegerField(
        required=False,
        initial=30,
        min_value=1,
        help_text="Maximum amount of time to wait for a new alert before stopping the streaming pull."
    )
    max_backlog = forms.IntegerField(
        required=False,
        initial=1000,
        min_value=1,
        help_text="Maximum number of pulled but unprocessed alerts before pausing the streaming pull."
    )


class BrokerStreamPython(GenericBroker):
    """Pitt-Google broker interface to pull alerts from Pub/Sub via the Python client."""

    name = "Pitt-Google StreamPython"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Pull or query alerts, unpack, apply the user filter, return an iterator."""
        clean_params = self._clean_parameters(parameters)

        alert_dicts_list = CONSUMER.stream_alerts(
            lighten_alerts=True,
            user_filter=self.user_filter,
            parameters=clean_params,
        )
        return iter(alert_dicts_list)

    def _clean_parameters(self, parameters):
        clean_params = dict(parameters)

        # there must be at least one stopping condition
        if (clean_params['max_results'] is None) & (clean_params['timeout'] is None):
            raise ValueError((
                "You must set at least one stopping condition. "
                "max_results and timeout cannot both be None."
            ))

        if clean_params['max_backlog'] is None:
            clean_params['max_backlog'] = 1000

        return clean_params

    @staticmethod
    def user_filter(alert_dict, parameters):
        """Apply the filter indicated by the form's parameters.

        Used as the `callback` to `BrokerStreamPython.unpack_and_ack_messages`.

        Args:
            `alert_dict`: Single alert, ZTF packet data as a dictionary.
                          The schema depends on the value of `lighten_alerts` passed to
                          `BrokerStreamPython.unpack_and_ack_messages`.
                          If `lighten_alerts=False` it is the original ZTF alert schema
                          (https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html).
                          If `lighten_alerts=True` the dict is flattened and extra
                          fields are dropped.

            `parameters`: parameters submitted by the user through the form.

        Returns:
            `alert_dict` if it passes the filter, else `None`
        """
        if parameters["classtar_threshold"] is None:
            # no filter requested. all alerts pass
            return alert_dict

        # run the filter
        lt_threshold = alert_dict["classtar"] < parameters["classtar_threshold"]
        if ((parameters["classtar_gt_lt"] == "lt") & lt_threshold) or (
            (parameters["classtar_gt_lt"] == "gt") & ~lt_threshold
        ):
            return alert_dict
        else:
            return None

    def to_generic_alert(self, alert):
        """Map the Pitt-Google alert to a TOM `GenericAlert`."""
        return GenericAlert(
            timestamp=jd_to_readable_date(alert["jd"]),
            url=CONSUMER.pull_url,
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["magpsf"],
            score=alert["classtar"],
        )
