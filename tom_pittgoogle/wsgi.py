"""
WSGI config for tommy project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/howto/deployment/wsgi/

WARNING: This file is only here for the Read The Docs build.
         It should not be used to run the TOM server.
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tom_pittgoogle.settings')

application = get_wsgi_application()
