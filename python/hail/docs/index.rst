.. Hail documentation master file, created by
   sphinx-quickstart on Fri Nov  4 10:55:10 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


========
Contents
========

.. toctree::
    :maxdepth: 2

    Installation <getting_started>
    Tutorials <tutorials-landing>
    Reference (Python API) <api>
    Hailpedia <overview>
    For Software Developers <getting_started_developing>
    Other Resources <other_resources>

==================
Indices and tables
==================

* :ref:`genindex`
* :ref:`search`



Filter loci by a list of locus intervals
----------------------------------------

From a table of intervals
.........................

:**tags**: :func:`.import_locus_intervals`, :meth:`.MatrixTable.filter_rows`

:**description**: Import a text file of locus intervals as a table, then use
                  this table to filter the loci in a matrix table.

:**code**:

    >>> interval_table = hl.import_locus_intervals('data/gene.interval_list')
    >>> filtered_mt = mt.filter_rows(hl.is_defined(interval_table[mt.locus]))

:**understanding**:

    .. container:: toggle

        .. container:: toggle-content

            We have a matrix table ``mt`` containing the loci we would like to filter, and a
            list of locus intervals stored in a file. We can import the intervals into a
            table with :func:`.import_locus_intervals`.

            Hail supports implicit joins between locus intervals and loci, so we can filter
            our dataset to the rows defined in the join between the interval table and our
            matrix table.

            ``interval_table[mt.locus]`` joins the matrix table with the table of intervals
            based on locus and interval<locus> matches. This is a StructExpression, which
            will be defined if the locus was found in any interval, or missing if the locus
            is outside all intervals.

            To do our filtering, we can filter to the rows of our matrix table where the
            struct expression ``interval_table[mt.locus]`` is defined.

            This method will also work to filter a table of loci, instead of
            a matrix table.

From a Python list
..................

:**tags**: :func:`.filter_intervals`

:**description**: Filter loci in a matrix table using a list of intervals.
                  Suitable for a small list of intervals.

:**code**:

        >>> interval_table = hl.import_locus_intervals('data/gene.interval_list')
        >>> interval_list = [x.interval for x in interval_table.collect()]
        >>> filtered_mt = hl.filter_intervals(mt, interval_list)