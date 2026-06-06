#----------------------------#
# Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam
#----------------------------#

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from main import TransactionSummary

class TestTransactionSummary(unittest.TestCase):
    def test_filters_and_sums(self):
        input_rows = [
            '2011-01-01 02:54:25 UTC, wallet1, wallet2, 100.0', # should be included
            '2011-01-01 02:54:25 UTC, wallet3, wallet4, 19.9', # shouldn't be included: less than 20
            '2009-01-01 02:54:25 UTC, wallet5, wallet6, 100.0', # shouldn't be included: before 2010
            '2012-01-01 02:54:25 UTC, wallet1, wallet2, 200.0', # should be included
        ]
        expected = [
            ('2011-01-01', 100.0),
            ('2012-01-01', 200.0)
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(input_rows)
                | TransactionSummary()
            )
            assert_that(result, equal_to(expected))

if __name__ == '__main__':
    unittest.main()