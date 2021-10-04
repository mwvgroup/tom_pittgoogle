#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""Pitt-Google module for TOM Toolkit. Retrieves alerts from the BigQuery database."""

from django import forms
from django.conf import settings
from google.cloud import bigquery
from google.oauth2 import service_account
from tom_alerts.alerts import GenericQueryForm, GenericAlert, GenericBroker

# from tom_alerts.models import BrokerQuery
# from tom_targets.models import Target
# import requests

from .utils.templatetags.utility_tags import jd_to_readable_date


CLIENT = bigquery.Client(
    project=settings.GOOGLE_CLOUD_PROJECT,
    credentials=service_account.Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS
    )
)

# ZTF_ALERTS_TABLE_NAME = "ardent-cycling-243415.ztf_alerts.alerts"
ZTF_ALERTS_TABLE_NAME = "ardent-cycling-243415.ztf_alerts_storebq.alerts"

# This module uses the BigQuery Python client to query the database.
# An alernative is to use the REST API and send a POST request to the following url,
# where ``{projectId}`` is replaced by the user's Google Cloud project Id.
# See: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
# We define this because the ``GenericAlert`` wants to record a url,
# but the end user will need either better instructions or a better url.
query_url = "https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/queries"


class FilterAlertsTableForm(GenericQueryForm):
    """Form for filtering a table of alerts.

    Fields:
        objectId (``CharField``)
        candid (``IntegerField``)
        max_results (``IntegerField``)
    """

    objectId = forms.CharField(required=False)
    candid = forms.IntegerField(required=False)
    max_results = forms.IntegerField(
        required=False, initial=10, min_value=1, max_value=100
    )


class PittGoogleBrokerDatabase(GenericBroker):
    """Pitt-Google broker interface to query the database."""

    name = "Pitt-Google database"
    form = FilterAlertsTableForm

    @classmethod
    def fetch_alerts(self, parameters):
        """."""
        clean_params = self._clean_parameters(parameters)

        sql_stmnt, job_config = self._create_sql_stmnt(clean_params)
        query = CLIENT.query(sql_stmnt, job_config=job_config)

        output = []
        for row in query.result():
            row = dict(row)
            row['jd'] = jd_to_readable_date(row['jd'])
            output.append(row)

        return iter(output)

    def _create_sql_stmnt(parameters):
        """Create the SQL statement and the query parameters job config."""
        # create each piece of the SQL statement and associated query parameters
        query_parameters = []

        select = """
            SELECT
                publisher,
                objectId,
                candid,
                CASE candidate.fid
                    WHEN 1 THEN 'g' WHEN 2 THEN 'R' WHEN 3 THEN 'i' END as filter,
                candidate.magpsf as mag,
                candidate.jd as jd,
                candidate.ra as ra,
                candidate.dec as dec,
                candidate.classtar as classtar
        """
        frm = f"FROM `{ZTF_ALERTS_TABLE_NAME}`"

        if parameters["objectId"]:
            where = "WHERE objectId=@objectId"
            query_parameters.append(
                bigquery.ScalarQueryParameter(
                    "objectId", "STRING", parameters["objectId"]
                )
            )
        elif parameters["candid"]:
            where = "WHERE candid=@candid"
            query_parameters.append(
                bigquery.ScalarQueryParameter("candid", "INT64", parameters["candid"])
            )
        else:
            where = ""

        orderby = "ORDER BY jd DESC"
        limit = "LIMIT @max_results"
        query_parameters.append(
            bigquery.ScalarQueryParameter(
                "max_results", "INT64", parameters["max_results"]
            )
        )

        # construct the SQL statement and job config
        sql_stmnt = " ".join([select, frm, where, orderby, limit])
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        return sql_stmnt, job_config

    def _clean_parameters(parameters):
        clean_params = dict(parameters)

        # make sure objectId and candid are not both set
        if (len(clean_params["objectId"]) > 0) & (clean_params["candid"] is not None):
            raise forms.ValidationError(
                "Only one of either objectId or candid can be used to filter."
            )

        # set max results, if None
        if not clean_params["max_results"]:
            clean_params["max_results"] = 10

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
