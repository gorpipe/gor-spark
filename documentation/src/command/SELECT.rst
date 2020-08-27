.. raw:: html

   <span style="float:right; padding-top:10px; color:#BABABA;">Used in: gor only</span>

.. _SELECT:

====
SPARK
====
The :ref:`SELECT` is an alias for a Spark SQL source. Under the hood its just calling the :ref:`SPARK` command with the same suffix.

Usage
=====

.. code-block:: gor

   create #tempfile# = select [attributes] * from source;
   gor [#tempfile#]

Options
=======

.. tabularcolumns:: |L|J|

Same options as in the :ref:`SPARK` command