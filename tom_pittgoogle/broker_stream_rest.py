#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google broker module for TOM Toolkit.

Connects to the user's Pub/Sub subscription via the REST API.

API Docs: https://cloud.google.com/pubsub/docs/reference/rest

.. autosummary::
   :nosignatures:

   tom_pittgoogle.broker_stream_rest.FilterAlertsForm
   tom_pittgoogle.broker_stream_rest.PittGoogleBrokerStreamRest
"""

from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_stream_rest import PittGoogleConsumer
from .utils.templatetags.utility_tags import jd_to_readable_date


SUBSCRIPTION_NAME = "ztf-loop"
# Create the subscription if needed, and make sure we can connect to it.
CONSUMER = PittGoogleConsumer(SUBSCRIPTION_NAME)


class FilterAlertsForm(GenericQueryForm):
    """Form for filtering alerts.

    Fields:
        max_results (``IntegerField``)
        classtar_threshold (``FloatField``)
        classtar_gt_lt (``ChoiceField``)
    """

    max_results = forms.IntegerField(
        required=True, initial=10, min_value=1, max_value=1000
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


class PittGoogleBrokerStreamRest(GenericBroker):
    """Pitt-Google broker interface to pull alerts from Pub/Sub via the REST API."""

    name = "Pitt-Google stream rest"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Pull or query alerts, unpack, apply the user filter, return an iterator."""
        clean_params = self._clean_parameters(parameters)

        alerts, i, max_tries = [], 0, 5  # avoid trying forever
        while (len(alerts) < parameters['max_results']) & (i < max_tries):
            i += 1
            print(i)
            print(len(alerts))
            clean_params['max_results'] = parameters['max_results'] - len(alerts)
            alerts += self._request_alerts(clean_params)  # List[dict]

        return iter(alerts)

    def _request_alerts(self, parameters):
        """Pull alerts using a POST request with OAuth, unpack, apply user filter.

        All messages (alerts) are automatically acknowledged, and therefore leave the
        subscription, unless there is an error.
        """
        response = CONSUMER.oauth.post(
            f"{CONSUMER.subscription_url}:pull",
            data={"maxMessages": parameters["max_results"]},
        )
        response.raise_for_status()
        alerts = CONSUMER.unpack_and_ack_messages(
            response,
            lighten_alerts=True,
            callback=self._user_filter,
            parameters=parameters,
        )  # List[dict]
        return alerts

    @staticmethod
    def _user_filter(alert_dict, parameters):
        """Apply the filter indicated by the form's parameters.

        Used as the `callback` to `CONSUMER.unpack_and_ack_messages`.

        Args:
            `alert_dict`: Single alert, ZTF packet data.
                          The schema depends on the value of `lighten_alerts` passed to
                          `CONSUMER.unpack_and_ack_messages`.
                          If `lighten_alerts=False` it is the original ZTF alert schema
                          (https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html)
                          If `lighten_alerts=True` the dict is flattened and only
                          includes keys saved by `CONSUMER._lighten_alert`.

            `parameters`: parameters submitted by the user through the form.

        Returns:
            `alert_dict` if it passes the filter, else `None`
        """
        if parameters["classtar_threshold"] is None:
            # no filter requested. all alerts pass
            return alert_dict

        # run the filter
        classtar_lt_thresh = alert_dict["classtar"] < parameters["classtar_threshold"]
        if ((parameters["classtar_gt_lt"] == "lt") & classtar_lt_thresh) or (
            (parameters["classtar_gt_lt"] == "gt") & ~classtar_lt_thresh
        ):
            return alert_dict
        else:
            return None

    def _clean_parameters(self, parameters):
        clean_params = dict(parameters)
        return clean_params

    def to_generic_alert(self, alert):
        """Map the Pitt-Google alert to a TOM `GenericAlert`."""
        return GenericAlert(
            timestamp=jd_to_readable_date(alert["jd"]),
            url=CONSUMER.subscription_url,
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["magpsf"],
            score=alert["classtar"],
        )
