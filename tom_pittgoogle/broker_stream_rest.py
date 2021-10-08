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
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_stream_rest import ConsumerStreamRest
from .utils.templatetags.utility_tags import jd_to_readable_date


class FilterAlertsForm(GenericQueryForm):
    """Basic form for filtering alerts.

    Fields:

        subscription_name (``CharField``)

        classtar_threshold (``FloatField``)

        classtar_gt_lt (``ChoiceField``)

        max_results (``IntegerField``)
    """

    subscription_name = forms.CharField(
        required=True,
        initial='ztf-loop',
        help_text=(
            "The subscription will be created if it doesn't already exist "
            "in the user's project. The ztf-loop stream is recommended for testing. "
            "It is a 'heartbeat' stream with ~1 alert/sec."
        )
    )
    classtar_threshold = forms.FloatField(
        required=False,
        min_value=0,
        max_value=1,
        help_text="Star/Galaxy score threshold",
    )
    classtar_gt_lt = forms.ChoiceField(
        required=True,
        choices=[("lt", "less than"), ("gt", "greater than or equal")],
        initial="lt",
        widget=forms.RadioSelect,
        label="",
    )
    max_results = forms.IntegerField(
        required=False,
        initial=100,
        min_value=1,
    )


class BrokerStreamRest(GenericBroker):
    """Pitt-Google broker class to pull alerts from a stream via the REST API.

    Base class: ``tom_alerts.alerts.GenericBroker``
    """

    name = "Pitt-Google StreamRest"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Entry point to pull and filter alerts."""
        clean_params = self._clean_parameters(parameters)

        self.consumer = ConsumerStreamRest(clean_params['subscription_name'])

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
        response = self.consumer.oauth2.post(
            f"{self.consumer.subscription_url}:pull",
            data={"maxMessages": parameters["max_results"]},
        )
        response.raise_for_status()
        alerts = self.consumer.unpack_and_ack_messages(
            response,
            lighten_alerts=True,
            callback=self.user_filter,
            parameters=parameters,
        )  # List[dict]
        return alerts

    @staticmethod
    def user_filter(alert_dict, parameters):
        """Apply the filter indicated by the form's parameters.

        Used as the `callback` to `BrokerStreamRest.unpack_and_ack_messages`.

        Args:
            `alert_dict`: Single alert, ZTF packet data as a dictionary.
                          The schema depends on the value of `lighten_alerts` passed to
                          `BrokerStreamRest.unpack_and_ack_messages`.
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

    def _clean_parameters(self, parameters):
        clean_params = dict(parameters)
        return clean_params

    def to_generic_alert(self, alert):
        """Map the Pitt-Google alert to a TOM `GenericAlert`."""
        return GenericAlert(
            timestamp=jd_to_readable_date(alert["jd"]),
            url=self.consumer.subscription_url,
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["magpsf"],
            score=alert["classtar"],
        )
