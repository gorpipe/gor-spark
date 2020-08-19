.. raw:: html

   <span style="float:right; padding-top:10px; color:#BABABA;">Used in: gor only</span>

.. _SPARK:

====
SPARK
====
The :ref:`SPARK` command loads the source into a Spark DataFrame. The pipe steps following the Spark source are translated into corresponding spark functions.

When no split attribute is provided, the SPARK command uses the build split configuration file to specify the split partitions.  Typically, such configuration files partition the larger chromosomes into two partitions, separated on the centromeres.
If any of the commands GROUP, GRANNO, RANK, or SORT are present in the query using binsize other than 1 base, only whole chromosomes (contigs) are used, avoiding boundary effect issues arising from partition of query range.
The cache result is a spark parquet directory.

Usage
=====

.. code-block:: gor

   create #tempfile# = spark [attributes] source;
   gor [#tempfile#]

Options
=======

.. tabularcolumns:: |L|J|

+------------------------------+---------------------------------------------------------------------------------+
| ``-split value[:overlap]``   |  If the split value is smaller or equal to 1000, it represents the number of    |
|                              |  partitions into which the genome is split.  If the split value is larger,      |
|                              |  it is the size of each split in sequence bases.                                |
|                              |                                                                                 |
|                              |  The overlap number can be provided optionally to allow genome partitions       |
|                              |  to overlap. In such cases, the variable ##WHERE_SPLIT_WINDOW## can be used     |
|                              |  to set filtering on the output.                                                |
|                              |                                                                                 |
|                              |  Note, when partitions are represented in bases, no more                        |
|                              |  than 100 partitions can be per chromosome.                                     |
+------------------------------+---------------------------------------------------------------------------------+