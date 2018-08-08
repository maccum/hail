import unittest

import hail as hl

from hail.plot.slippyplot.manhattan_preprocessor import ManhattanPreprocessor

from test.hail.helpers import *

setUpModule = startTestHailContext
tearDownModule = stopTestHailContext


class Tests(unittest.TestCase):

    def setUp(self):
        schema = hl.tstruct(locus=hl.tstr, phenotype=hl.tstr,
                            pval=hl.tfloat32)
        ht = hl.Table.parallelize([
            {'locus': '1:904165', 'phenotype': 'height', 'pval': 0.1},
            {'locus': '1:909917', 'phenotype': 'height', 'pval': 0.002},
        ], schema)
        ht = ht.annotate(locus=hl.parse_locus(ht.locus))

        self.mt = ht.to_matrix_table(['locus'], ['phenotype'])

    def test_adding_manhattan_data(self):
        mt = self.mt
        mp = ManhattanPreprocessor(mt.locus, mt.phenotype, mt.pval)
        manhat_mt = mp.add_manhattan_data()

        expected_color = mp.colors['1']
        expected_min_nlp = -hl.log(0.1)
        expected_max_nlp = -hl.log(0.002)

        entries = (hl.Table.parallelize([
            {'locus': '1:904165', 'phenotype': 'height', 'pval': 0.1,
             'global_position': 904164, 'color': expected_color,
             'neg_log_pval': -hl.log(0.1)},
            {'locus': '1:909917', 'phenotype': 'height', 'pval': 0.002,
             'global_position': 909916, 'color': expected_color,
             'neg_log_pval': -hl.log(0.002)}],
            hl.tstruct(locus=hl.tstr, phenotype=hl.tstr,
                       pval=hl.tfloat32, global_position=hl.tint64,
                       color=hl.tstr,
                       neg_log_pval=hl.tfloat64)
        ))
        expected_mt = (entries.annotate(locus=hl.parse_locus(entries.locus))
                       .to_matrix_table(['locus'], ['phenotype'],
                                        ['global_position', 'color'])
                       .annotate_cols(min_nlp=expected_min_nlp,
                                      max_nlp=expected_max_nlp)
                       .annotate_globals(
            gp_range=hl.struct(min=904164, max=909916)))

        self.assertTrue(manhat_mt._same(expected_mt))