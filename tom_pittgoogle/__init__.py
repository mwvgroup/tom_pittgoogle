#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""The `tom_pittgoogle` module contains Pitt-Google's TOM broker implementations."""

from django.core.wsgi import get_wsgi_application
import os


if 'BUILD_IN_RTD' in os.environ:
    os.environ['DJANGO_SETTINGS_MODULE'] = "tom_pittgoogle.settings"
    application = get_wsgi_application()
