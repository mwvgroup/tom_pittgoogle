# !/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""Tests for the ``templatetags.utility_tags`` module."""

from django.test import TestCase

from broker_web.apps.utils.templatetags.utility_tags import jd_to_readable_date
from broker_web.apps.utils.templatetags.utility_tags import urlparams


class JDToReadableDate(TestCase):
    """Tests for the ``jd_to_readable_date`` tag"""

    def test_date_conversion(self):
        """Test the date is returned as a string for a pre-chosen JD"""

        jd = 2459145.19
        readable_string = '22 Oct 2020 - 16:33:36'
        self.assertEqual(jd_to_readable_date(jd), readable_string)


class URLParams(TestCase):
    """Tests for the ``urlparams`` tag"""

    def test_empty_args(self):
        """Test an empty string is returned for empty kwargs"""

        self.assertEqual(urlparams(), '')

    def test_multiple_kwargs_combined_with_ampersand(self):
        """Test multiple arguments are combined with & symbol"""

        kwargs = {'arg1': 'val1', 'arg2': 'val2'}
        expected_return = '?arg1=val1&arg2=val2'
        self.assertEqual(urlparams(**kwargs), expected_return)
