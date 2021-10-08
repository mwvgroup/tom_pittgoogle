#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""TOM Toolkit broker to query a BigQuery table via the Python API.

Relies on `ConsumerDatabasePython` to manage the connections and work with data.

See especially:

.. autosummary::
   :nosignatures:

   BrokerDatabasePython.request_alerts
"""

from django import forms
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

from .consumer_database_python import ConsumerDatabasePython
from .utils.templatetags.utility_tags import jd_to_readable_date


class FilterAlertsForm(GenericQueryForm):
    """Basic form for filtering alerts; currently implemented in the SQL statement.

    Fields:
        objectId (``CharField``)

        candid (``IntegerField``)

        max_results (``IntegerField``)
    """

    objectId = forms.CharField(required=False)
    candid = forms.IntegerField(required=False)
    max_results = forms.IntegerField(
        required=True, initial=100, min_value=1
    )


class BrokerDatabasePython(GenericBroker):
    """Pitt-Google broker to query alerts from the database via the Python client.

    Base class: ``tom_alerts.alerts.GenericBroker``
    """

    name = "Pitt-Google DatabasePython"
    form = FilterAlertsForm

    def fetch_alerts(self, parameters):
        """Entry point to query and filter alerts."""
        clean_params = self._clean_parameters(parameters)

        table_name = "ztf_alerts.alerts"
        self.consumer = ConsumerDatabasePython(table_name)

        alerts = self.request_alerts(clean_params)

        return iter(alerts)

    def request_alerts(self, parameters):
        """Query alerts using the user filter and unpack.

        The SQL statement returned by the `Consumer` implements the current user filter.

        Returns:
            alerts (List[dict])
        """
        sql_stmnt, job_config = self.consumer.create_sql_stmnt(parameters)
        query_job = self.consumer.client.query(sql_stmnt, job_config=job_config)
        alerts = self.consumer.unpack_query(query_job)  # List[dict]
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
        """Map the Pitt-Google alert to a TOM `GenericAlert`."""
        return GenericAlert(
            timestamp=jd_to_readable_date(alert["jd"]),
            url=self.consumer.query_url,
            id=alert["candid"],
            name=alert["objectId"],
            ra=alert["ra"],
            dec=alert["dec"],
            mag=alert["mag"],
            score=alert["classtar"],
        )
