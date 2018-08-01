Matrix Table Joins
==================

Hail currently supports inner and left joins between matrix tables,
on either the rows or columns. Outer joins are not yet implemented.

Union of Columns (Inner Join on Rows) Between Matrix Tables
-----------------------------------------------------------

>>> joined_mt = mt1.union_cols(mt2)

:meth:`.MatrixTable.union_cols` takes the union of the columns of two matrix
tables by performing an inner join on their rows. The resulting matrix table
will have the row fields from ``mt1``, and the concatenated entries from
both matrix tables.

Union of Rows (Inner Join on Columns) Between Matrix Tables
-----------------------------------------------------------

>>> joined_mt = mt1.union_rows(mt2)

:meth:`.MatrixTable.union_rows` takes the union of the rows of two matrix
tables by performing an inner join on their columns. The resulting matrix
table will have the column fields from ``mt1``.

Left Join Between Matrix Table and Table
----------------------------------------

>>> joined_mt = mt.annotate_rows(table_fields = table[mt.locus, mt.alleles])

Annotating a matrix table with the rows of a table performs a left join on
the rows. Here, ``table`` is keyed by two fields, locus and alleles, which match
the row keys of ``mt``. We construct an expression
``table[mt.locus, mt.alleles]`` which refers to the row of ``table`` with key
values ``mt.locus`` and ``mt.alleles``. We call
:meth:`.MatrixTable.annotate_rows` to annotate ``mt`` with this expression,
which results in a new matrix table with an additional field ``table_fields``,
which is a struct containing the nested fields from ``table``.

>>> joined_mt = mt.annotate_rows(**table[mt.locus, mt.alleles])

If we want to add the fields from ``table`` to ``mt`` as top level fields,
rather than adding them as a struct, then we can use the ** syntax to unpack
the row from ``table``.

>>> joined_mt = mt.annotate_rows(table_field_x = table[mt.locus, mt.alleles].x)

We can add a new row field from the table by selecting the field from
the expression like so: ``table[mt.locus, mt.alleles].x``. This results in a
new matrix table with one new row field, ``x``.

Joins Can Be Used to Filter Matrix Tables
-----------------------------------------

>>> joined_mt = mt.filter_rows(hl.is_defined(table[mt.locus, mt.alleles]))

To filter a matrix table ``mt`` to the rows where ``mt``'s key is defined in
``table``, we can use a join. The expression ``table[mt.locus, mt.alleles]``
refers to the rows of ``table`` with key values ``mt.locus`` and ``mt.alleles``.
We can filter to just those rows that are defined using :func:`.is_defined` and
:meth:`.MatrixTable.filter_rows`.
