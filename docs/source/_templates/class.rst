{{ fullname | escape | underline }}
.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   {% set attributes = (attributes | reject("in", inherited_members) | list) %}

   {% block attributes %}

   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   {% for item in attributes %}
   .. autoattribute:: {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% set methods = (methods | reject("equalto", "__init__") | reject("in", inherited_members) | list) %}

   {% block methods %}

   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   {% for item in methods %}
   .. automethod:: {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}
