Snowpark Checkpoints Framework API Reference
============================================

The Snowpark Checkpoints Framework is a Python package that provides an integrated solution for validating the compatibility between PySpark and migrated Snowpark workloads.

It lets you collect information about PySpark dataframes using a collector mechanism. The collected information is then used to validate the generated Snowpark dataframes to ensure that they are equivalent to the original PySpark dataframes.

.. toctree::
   :maxdepth: 3

   snowpark-checkpoints-collectors/index
   snowpark-checkpoints-configuration/index
   snowpark-checkpoints-hypothesis/index
   snowpark-checkpoints-validators/index
