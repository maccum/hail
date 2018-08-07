import pytest
import os
import hail as hl
import hail.expr.aggregators as agg

@pytest.fixture(scope="module", autouse=True)
def init(doctest_namespace):
    olddir = os.getcwd()
    os.chdir("docs/")

    doctest_namespace['hl'] = hl
    doctest_namespace['agg'] = agg

    doctest_namespace['left'] = hl.Table.parallelize([
        {'k': 0, 'x': 10},
        {'k': 1, 'x': 12},
        {'k': 1, 'x': 2},
        {'k': 2, 'x': 5}],
        hl.tstruct(k=hl.tint32, x=hl.tint32),
        key='k')
    doctest_namespace['right'] = hl.Table.parallelize([
        {'id': 1, 'y': .5},
        {'id': 2, 'y': .13},
        {'id': 2, 'y': -.01},
        {'id': 3, 'y': .07}],
        hl.tstruct(id=hl.tint32, y=hl.tfloat32),
        key='id')



    os.chdir(olddir)
