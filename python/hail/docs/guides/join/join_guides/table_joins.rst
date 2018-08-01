Joining Tables
==============

Joins let us combine multiple datasets together.

The join syntax most commonly used in Hail is
``right_table[left_table.field_in_left_table]``.

An expression of the format ``table[expr]`` refers to the row of ``table`` with
value ``expr``.

If ``expr`` is an expression indexed by the row of another table, any operation
which uses ``expr`` will perform a join.


Inner Join
----------

>>> joined_table = left.key_by('k').join(right.key_by('id'))

:meth:`.Table.join` performs an inner join by default. Both tables must have the
same number of keys, the same key types, and the same order of keys, although
the key names may be different.


Outer Join
----------

>>> joined_table = left.join(right, how='outer')

Outer joins can be done with :meth:`.Table.join` by passing the parameter
``how = 'outer'``. Both tables must have the same number of keys, the same key
types, and the same order of keys, although the key names may be different.


Left Join
---------

Using expression language
~~~~~~~~~~~~~~~~~~~~~~~~~

>>> joined_table = left.annotate(fields_from_right = right[left.key])

Left joins can be done by annotating a table with fields from another table.
In this snippet of code, ``right[left.key]`` is a StructExpression that
creates a mapping from the keys in the left table to the rows in the right
table. The resulting table will have the fields of ``left`` plus an additional
field, ``fields_from_right``, of type struct, which contains nested fields
from ``right``.

Keeping a specific field from the right table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

>>> joined_table = left.annotate(y = right[left.key].y)

Performs a left join where only one field, ``y``, from the right table is added
to the left table.

With the join() method
~~~~~~~~~~~~~~~~~~~~~~

>>> joined_table = left.join(right, how = 'left')

Left joins can be done with :meth:`.Table.join` by passing the parameter
``how = 'left'``. Both tables must have the same number of keys, the same key
types, and the same order of keys, although the key names may be different.

Right Join
----------

>>> joined_table = left.join(right, how = 'right')

Right joins can be done with :meth:`.Table.join` by passing the parameter
``how = 'right'``. Both tables must have the same number of keys, the same
key types, and the same order of keys, although the key names may be different.



