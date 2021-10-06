#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google broker module for TOM Toolkit.

Connects to Pitt-Google's BigQuery table via the Python client.

API Docs: https://googleapis.dev/python/bigquery/latest/reference.html

.. autosummary::
   :nosignatures:

   tom_pittgoogle.broker_database_python.FilterAlertsForm
   tom_pittgoogle.broker_database_python.PittGoogleBroker

"""

from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_database_python import ConsumerDatabasePython
from .utils.templatetags.utility_tags import jd_to_readable_date


TABLE_NAME = "alerts"
CONSUMER = ConsumerDatabasePython(TABLE_NAME)


class FilterAlertsForm(GenericQueryForm):
    """Form for filtering a table of alerts.

    Fields:
        objectId (``CharField``)

        candid (``IntegerField``)

        max_results (``IntegerField``)
    """

    objectId = forms.CharField(required=False)
    candid = forms.IntegerField(required=False)
    max_results = forms.IntegerField(
        required=True, initial=10, min_value=1
    )


class PittGoogleBrokerDatabasePython(GenericBroker):
    """Pitt-Google broker interface to query the database."""

    name = "Pitt-Google database python"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Query alerts using the user filter, unpack, return an iterator."""
        clean_params = self._clean_parameters(parameters)
        alerts = self._request_alerts(clean_params)
        return iter(alerts)

    def _request_alerts(self, parameters):
        """Query alerts using the user filter and unpack.

        The current user filter is implemented in the SQL statement, so there
        is no need to use a `callback`, or to call this function more than once.
        """
        sql_stmnt, job_config = CONSUMER.create_sql_stmnt(parameters)
        query_job = CONSUMER.client.query(sql_stmnt, job_config=job_config)
        alerts = CONSUMER.unpack_query(query_job, callback=None)  # List[dict]
        return alerts

    def _clean_parameters(self, parameters):
        clean_params = dict(parameters)

        # make sure objectId and candid are not both set
        if (len(clean_params["objectId"]) > 0) & (clean_params["candid"] is not None):
            raise forms.ValidationError(
                "Only one of either objectId or candid can be used to filter."
            )

        return clean_params

    def to_generic_alert(self, alert):
        """."""
        return GenericAlert(
            timestamp=jd_to_readable_date(alert["jd"]),
            url=CONSUMER.query_url,
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["mag"],
            score=alert["classtar"],
        )
