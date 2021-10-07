#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""TOM Toolkit broker to listen to a Pitt-Google Pub/Sub stream via the REST API.

Relies on `ConsumerStreamRest` to manage the connections and work with data.

See especially:

.. autosummary::
   :nosignatures:

   BrokerStreamRest.request_alerts
   BrokerStreamRest.user_filter
"""

from django import forms
import os
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_stream_rest import ConsumerStreamRest
from .utils.templatetags.utility_tags import jd_to_readable_date


SUBSCRIPTION_NAME = "ztf-loop"  # heartbeat stream. ~1 ZTF alert/second.
# TODO: Connect the OAuth to a Django page,
# and move the `CONSUMER` instantiation to a more appropriate place in the logic
# so the end user can authenticate.
# Currently, the developer must authticate when launching the server.
if 'BUILD_IN_RTD' not in os.environ:
    CONSUMER = ConsumerStreamRest(SUBSCRIPTION_NAME)


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


class BrokerStreamRest(GenericBroker):
    """Pitt-Google broker to pull alerts from a stream via the REST API."""

    name = "Pitt-Google stream rest"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Entry point to pull and filter alerts."""
        clean_params = self._clean_parameters(parameters)

        alerts, i, max_tries = [], 0, 5  # avoid trying forever
        while (len(alerts) < parameters['max_results']) & (i < max_tries):
            i += 1
            clean_params['max_results'] = parameters['max_results'] - len(alerts)
            alerts += self.request_alerts(clean_params)  # List[dict]

        return iter(alerts)

    def request_alerts(self, parameters):
        """Pull alerts using a POST request with OAuth2, unpack, apply user filter.

        Returns:
            alerts (List[dict])
        """
        response = CONSUMER.oauth2.post(
            f"{CONSUMER.subscription_url}:pull",
            data={"maxMessages": parameters["max_results"]},
        )
        response.raise_for_status()
        alerts = CONSUMER.unpack_and_ack_messages(
            response,
            lighten_alerts=True,
            callback=self.user_filter,
            parameters=parameters,
        )  # List[dict]
        return alerts

    @staticmethod
    def user_filter(alert_dict, parameters):
        """Apply the filter indicated by the form's parameters.

        Used as the `callback` to `CONSUMER.unpack_and_ack_messages`.

        Args:
            `alert_dict`: Single alert, ZTF packet data as a dictionary.
                          The schema depends on the value of `lighten_alerts` passed to
                          `CONSUMER.unpack_and_ack_messages`.
                          If `lighten_alerts=False` it is the original ZTF alert schema
                          (https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html)
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
