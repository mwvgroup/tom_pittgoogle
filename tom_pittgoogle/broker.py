#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google broker module for TOM Toolkit."""

from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from consumer import PittGoogleConsumer


CONSUMER = PittGoogleConsumer()


class FilterAlertsForm(GenericQueryForm):
    """Form for filtering alerts.

    Fields:
        max_results (``IntegerField``)
    """

    max_results = forms.IntegerField(
        required=True, initial=10, min_value=1, max_value=1000
    )


class PittGoogleBroker(GenericBroker):
    """Pitt-Google broker interface to fetch alerts."""

    name = "Pitt-Google"
    form = FilterAlertsForm

    @classmethod
    def fetch_alerts(self, parameters):
        """Pull or query alerts, unpack, apply the user filter, return an iterator."""
        clean_params = self._clean_parameters(parameters)

        alerts = []
        while len(alerts) < parameters['max_results']:
            alerts += self._request_alerts(clean_params)  # List[dict]

        return iter(alerts)

    def _request_alerts(self, parameters):
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

    def _clean_parameters(parameters):
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
