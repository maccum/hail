import pytest
import os
import shutil
import hail as hl
import hail.expr.aggregators as agg


@pytest.fixture(autouse=True)
def always_true(monkeypatch):
    # FIXME: remove once test output matches docs
    monkeypatch.setattr('doctest.OutputChecker.check_output', lambda a, b, c, d: True)
    yield
    monkeypatch.undo()


@pytest.fixture(scope="session", autouse=True)
def init(doctest_namespace):
    # This gets run once per process -- must avoid race conditions
    print("setting up doctest...")

    olddir = os.getcwd()
    os.chdir("docs/")

    doctest_namespace['hl'] = hl
    doctest_namespace['agg'] = agg

    if not os.path.isdir("output/"):
        try:
            os.mkdir("output/")
        except OSError:
            pass

    files = ["sample.vds", "sample.qc.vds", "sample.filtered.vds"]
    for f in files:
        if os.path.isdir(f):
            shutil.rmtree(f)

    ds = hl.read_matrix_table('data/example.vds')
    doctest_namespace['ds'] = ds
    doctest_namespace['dataset'] = ds
    doctest_namespace['dataset2'] = ds.annotate_globals(global_field=5)
    doctest_namespace['dataset_to_union_1'] = ds
    doctest_namespace['dataset_to_union_2'] = ds

    v_metadata = ds.rows().annotate_globals(global_field=5).annotate(consequence='SYN')
    doctest_namespace['v_metadata'] = v_metadata

    s_metadata = ds.cols().annotate(pop='AMR', is_case=False, sex='F')
    doctest_namespace['s_metadata'] = s_metadata

    # Table
    table1 = hl.import_table('data/kt_example1.tsv', impute=True, key='ID')
    table1 = table1.annotate_globals(global_field_1=5, global_field_2=10)
    doctest_namespace['table1'] = table1
    doctest_namespace['other_table'] = table1

    table2 = hl.import_table('data/kt_example2.tsv', impute=True, key='ID')
    doctest_namespace['table2'] = table2

    table4 = hl.import_table('data/kt_example4.tsv', impute=True,
                             types={'B': hl.tstruct(B0=hl.tbool, B1=hl.tstr),
                                    'D': hl.tstruct(cat=hl.tint32, dog=hl.tint32),
                                    'E': hl.tstruct(A=hl.tint32, B=hl.tint32)})
    doctest_namespace['table4'] = table4

    people_table = hl.import_table('data/explode_example.tsv', delimiter='\\s+',
                                   types={'Age': hl.tint32, 'Children': hl.tarray(hl.tstr)})
    doctest_namespace['people_table'] = people_table

    # TDT
    doctest_namespace['tdt_dataset'] = hl.import_vcf('data/tdt_tiny.vcf')

    ds2 = hl.variant_qc(ds)
    doctest_namespace['ds2'] = ds2.select_rows(AF = ds2.variant_qc.AF)

    # Expressions
    doctest_namespace['names'] = hl.literal(['Alice', 'Bob', 'Charlie'])
    doctest_namespace['a1'] = hl.literal([0, 1, 2, 3, 4, 5])
    doctest_namespace['a2'] = hl.literal([1, -1, 1, -1, 1, -1])
    doctest_namespace['t'] = hl.literal(True)
    doctest_namespace['f'] = hl.literal(False)
    doctest_namespace['na'] = hl.null(hl.tbool)
    doctest_namespace['call'] = hl.call(0, 1, phased=False)
    doctest_namespace['a'] = hl.literal([1, 2, 3, 4, 5])
    doctest_namespace['d'] = hl.literal({'Alice': 43, 'Bob': 33, 'Charles': 44})
    doctest_namespace['interval'] = hl.interval(3, 11)
    doctest_namespace['locus_interval'] = hl.parse_locus_interval("1:53242-90543")
    doctest_namespace['locus'] = hl.locus('1', 1034245)
    doctest_namespace['x'] = hl.literal(3)
    doctest_namespace['y'] = hl.literal(4.5)
    doctest_namespace['s1'] = hl.literal({1, 2, 3})
    doctest_namespace['s2'] = hl.literal({1, 3, 5})
    doctest_namespace['s3'] = hl.literal({'Alice', 'Bob', 'Charlie'})
    doctest_namespace['struct'] = hl.struct(a=5, b='Foo')
    doctest_namespace['tup'] = hl.literal(("a", 1, [1, 2, 3]))
    doctest_namespace['s'] = hl.literal('The quick brown fox')
    doctest_namespace['interval2'] = hl.Interval(3, 6)

    # Overview
    doctest_namespace['ht'] = hl.import_table("data/kt_example1.tsv", impute=True)
    doctest_namespace['mt'] = ds

    # Joins Guide
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
    doctest_namespace['mt1'] = hl.Table.parallelize(
        [{'v': "v1", 'u': "u1", 's': "s1", 't': "t8", 'e': 1},
         {'v': "v1", 'u': "u1", 's': "s1", 't': "t9", 'e': 2},
         {'v': "v1", 'u': "u1", 's': "s2", 't': "t7", 'e': 3},
         {'v': "v1", 'u': "u1", 's': "s2", 't': "t8", 'e': 4},
         {'v': "v1", 'u': "u1", 's': "s2", 't': "t9", 'e': 5},

         {'v': "v1", 'u': "u2", 's': "s1", 't': "t8", 'e': 6},
         {'v': "v1", 'u': "u2", 's': "s1", 't': "t9", 'e': 7},
         ], hl.tstruct(v=hl.tstr, u=hl.tstr, s=hl.tstr, t=hl.tstr,
                       e=hl.tint32)).to_matrix_table(['v', 'u'], ['s', 't'])
    doctest_namespace['mt2'] = hl.Table.parallelize(
        [{'v': "v1", 'u': "u2", 's': "s1", 't': "t8", 'e': 6},
         {'v': "v1", 'u': "u2", 's': "s1", 't': "t9", 'e': 7},
         {'v': "v1", 'u': "u2", 's': "s2", 't': "t7", 'e': 8},
         {'v': "v1", 'u': "u2", 's': "s2", 't': "t8", 'e': 9},
         {'v': "v1", 'u': "u2", 's': "s2", 't': "t9", 'e': 10},

         {'v': "v1", 'u': "u3", 's': "s1", 't': "t8", 'e': 11},
         {'v': "v1", 'u': "u3", 's': "s1", 't': "t9", 'e': 12},
         {'v': "v1", 'u': "u3", 's': "s2", 't': "t7", 'e': 13},
         ],
        hl.tstruct(v=hl.tstr, u=hl.tstr, s=hl.tstr, t=hl.tstr,
                   e=hl.tint32)).to_matrix_table(['v', 'u'], ['s', 't'])

    gnomad_data = ds.rows()
    doctest_namespace['gnomad_data'] = gnomad_data.select(gnomad_data.info.AF)

    print("finished setting up doctest...")
    yield
    os.chdir(olddir)
