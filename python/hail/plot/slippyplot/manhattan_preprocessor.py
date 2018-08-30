import hail as hl
from hail.expr.expressions.expression_utils import check_col_indexed
from hail.typecheck import *
from hail.expr.expressions import *


class ManhattanPreprocessor(object):

    def __init__(self, locus_expr, phenotype_expr, pval_expr):
        check_row_indexed('ManhattanPreprocessor', locus_expr)
        check_col_indexed('ManhattanPreprocessor', phenotype_expr)
        check_entry_indexed('ManhattanPreprocessor', pval_expr)

        self.mt = matrix_table_source('ManhattanPreprocessor', locus_expr)
        self.locus_field = self.mt._fields_inverse[locus_expr]
        self.phenotype_field = self.mt._fields_inverse[phenotype_expr]
        self.pval_field = self.mt._fields_inverse[pval_expr]

        self.colors = {
            '1': "#08ad4d", '2': "#cc0648", '3': "#bbdd11", '4': "#4a87d6",
            '5': "#6f50b7", '6': "#e0c10f", '7': "#d10456", '8': "#2779d8",
            '9': "#9e0631", '10': "#5fcc06", '11': "#4915a8", '12': "#0453d3",
            '13': "#7faf26", '14': "#d17b0c", '15': "#526d13", '16': "#e82019",
            '17': "#125b07", '18': "#12e2c3", '19': "#914ae2", '20': "#95ce10",
            '21': "#af1ca8", '22': "#eaca3a", 'X': "#1c8caf"}

    def add_manhattan_data(self, colors=None, threshold=.001):
        """
        Annotate the matrix table with data needed for manhattan plotting.

        * global locus positions (x-axis values)
        * negative log of p_values (y-axis values)
        * colors (because matplotlib will not take a color map)
        * min and max global position (x-axis range)
        * min and max -log(p_value) for each phenotype (y-axis ranges)


        :param threshold: p-value threshold for hover data
        :param colors: :class:`dict` of contigs to hex colors
        :return: :class:`.MatrixTable`
        """
        if not colors:
            colors = self.colors

        # add global_positions and colors
        mt = self.mt.annotate_globals(color_dict=colors)
        locus_expr = mt._fields[self.locus_field]
        mt = (mt
              .annotate_rows(global_position=locus_expr.global_position(),
                             color=mt.color_dict[locus_expr.contig])
              .drop('color_dict'))

        pval_expr = mt._fields[self.pval_field]
        label_expr = hl.array([hl.str(mt.locus),
                              hl.str(mt.alleles),
                              mt.gene,
                              hl.str(pval_expr)])
        mt = (mt.annotate_entries(neg_log_pval=-hl.log(pval_expr),
                                  under_threshold=pval_expr < threshold,
                                  label=label_expr)
              .key_cols_by(self.phenotype_field))

        # y-axis range for each phenotype
        mt = (mt.annotate_cols(min_nlp=hl.agg.min(mt.neg_log_pval),
                               max_nlp=hl.agg.max(mt.neg_log_pval)))

        # global position range (x-axis range)
        gp_range = mt.aggregate_rows(
            hl.struct(
                min=hl.agg.min(mt.global_position),
                max=hl.agg.max(mt.global_position)
            ))

        mt = mt.annotate_globals(gp_range=gp_range)

        return mt
