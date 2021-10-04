# !/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""General utilities used across apps

Paginating JSON Responses
-------------------------

The ``paginate_to_json`` function handles the generation of JSON responses to
HTTP requests for pagination of data. For example, it can be used in a view as:

.. code-block:: python
   :linenos:

   from django.views.generic import View

   from apps.utils import paginate_to_json

   class MyView(View):

       template = 'myt_template_path.html'

       def get(self, request, *args, **kwargs):
           data = [{'field1': 'value1'}, {'field1': 'value2'}, ...]
           return paginate_to_json(request, data)
"""

from django.http import JsonResponse


def paginate_to_json(request, data):
    """Paginate a list of dicts and return as a ``JsonResponse``

    For expected in/outputs of paginated data, see
    https://datatables.net/manual/server-side .

    Args:
        request (HttpRequest): Incoming HTTP request
        data           (list): The data to paginate
    """

    start = int(request.GET.get('start', 0))
    length = int(request.GET.get('length', 10))
    draw = request.GET.get('draw', -1)
    limit = request.GET.get('limit', None)

    if limit:
        limit = int(limit)

    # Paginate data
    data = data[:limit]
    paginated_alerts = data[start:start + length]

    response = {
        'draw': draw,
        'data': paginated_alerts,
        'recordsTotal': len(data),
        'recordsFiltered': len(data),
    }

    return JsonResponse(response)
