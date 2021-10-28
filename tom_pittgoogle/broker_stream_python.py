#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""TOM Toolkit broker to listen to a Pitt-Google Pub/Sub stream via the Python client.

Relies on `ConsumerStreamPython` to manage the connections and work with data.

See especially:

.. autosummary::
   :nosignatures:

   BrokerStreamPython.fetch_alerts
   BrokerStreamPython.user_filter

"""
from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker
from tom_targets.models import Target

from .consumer_stream_python import ConsumerStreamPython
from .utils.templatetags.utility_tags import jd_to_readable_date


class FilterAlertsForm(GenericQueryForm):
    """Basic form for filtering alerts.

    Fields:

        subscription_name (``CharField``)

        classtar_threshold (``FloatField``)

        classtar_gt_lt (``ChoiceField``)

        max_results (``IntegerField``)

        timeout (``IntegerField``)

        max_backlog (``IntegerField``)
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
        help_text=(
            "Maximum number of alerts to pull and process before stopping the "
            "streaming pull. Recommended for testing only."
        )
    )
    timeout = forms.IntegerField(
        required=False,
        initial=30,
        min_value=1,
        help_text=(
            "Maximum amount of time in seconds to wait for a new alert before stopping "
            "the streaming pull. Recommended for testing to avoid waiting for "
            "max number of results forever. "
            "Recommended for production, possibly in combination "
            "with a time of day-based stopping condition."
        )
    )
    max_backlog = forms.IntegerField(
        required=False,
        initial=1000,
        min_value=1,
        help_text=(
            "Maximum number of pulled but unprocessed alerts before pausing the "
            "streaming pull. Google's default is 1000, which is often fine. "
            "However, you may want to reduce this if you set 'Max results' to a "
            "small number and you want to avoid pulling down a bunch of extra alerts. "
            "Note that alerts do not 'leave' the subscription until they are "
            "successfully processed by the callback; this setting does not affect that."
        )
    )


class BrokerStreamPython(GenericBroker):
    """Pitt-Google broker interface to pull alerts from Pub/Sub via the Python client.

    Base class: ``tom_alerts.alerts.GenericBroker``
    """

    name = "Pitt-Google StreamPython"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Entry point to pull and filter alerts.

        Pull alerts using a Python client, unpack, apply user filter.

        This demo assumes that the real use-case is to save alerts to a database
        rather than view them through a TOM site.
        Therefore, the `Consumer` currently saves the alerts in real time,
        and then simply returns a list of alerts after all messages are processed.
        That list is then coerced into an iterator here.
        If the user really cares about the iterator,
        `ConsumerStreamPython.stream_alerts` can be tweaked to yield the alerts in
        real time.
        """
        clean_params = self._clean_parameters(parameters)

        self.consumer = ConsumerStreamPython(clean_params['subscription_name'])

        alert_dicts_list = self.consumer.stream_alerts(
            user_filter=self.user_filter,
            **clean_params,
        )

        return iter(alert_dicts_list)

    def _clean_parameters(self, parameters):
        clean_params = dict(parameters)

        clean_params.setdefault("classtar_threshold", None)

        # there must be at least one stopping condition
        if (clean_params['max_results'] is None) & (clean_params['timeout'] is None):
            raise ValueError((
                "You must set at least one stopping condition. "
                "max_results and timeout cannot both be None."
            ))

        if clean_params['max_backlog'] is None:
            clean_params['max_backlog'] = 1000  # keep the google default of 1000

        return clean_params

    @staticmethod
    def user_filter(alert_dict, **parameters):
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

    def to_generic_alert(self, alert_dict):
        """Map the Pitt-Google alert to a TOM `GenericAlert`."""
        return GenericAlert(
            timestamp=jd_to_readable_date(alert_dict["jd"]),
            # url=self.consumer.pull_url,
            # this is not a valid url
            url="https://pubsub.googleapis.com/v1/{subscription_path}",
            id=alert_dict["candid"],
            name=alert_dict["objectId"],
            ra=alert_dict["ra"],
            dec=alert_dict["dec"],
            mag=alert_dict["magpsf"],
            score=alert_dict["classtar"],
        )

    def to_target(self, alert_dict):
        """Map the Pitt-Google alert to a TOM `Target`."""
        return Target(
            # identifier=alert_dict['candid'],
            name=alert_dict['objectId'],
            type='SIDEREAL',
            # designation='MY ALERT',
            ra=alert_dict['ra'],
            dec=alert_dict['dec'],
            # epoch=alert_dict['jd'],
        )

    # def get_or_create_target(self, alert_dict):
    #     target, created = Target.objects.get_or_create(
    #         name=alert_dict['objectId'],
    #         type='SIDEREAL',
    #         ra=alert_dict['ra'],
    #         dec=alert_dict['dec'],
    #     )
