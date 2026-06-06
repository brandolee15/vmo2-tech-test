#----------------------------#
# Overall requirement
# 1. Once the solution is finished, please store it in a public Git repository on GitHub (this is free to create) and share the link with us
# 2. The solution should be working e2e, and ideally we would expect to clone the repo and run a single command to get the output.
# 3. You do not have to use Cloud Dataflow, Direct Runner is fine
# Task 1
# Write an Apache Beam batch job in Python satisfying the following requirements 1. Read the input from gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv 1. Find all transactions have a transaction_amount greater than 20 1. Exclude all transactions made before the year 2010 1. Sum the total by date 1. Save the output into output/results.jsonl.gz and make sure all files in the output/ directory is git ignored
# If the output is in a CSV file, it would have the following format
# date, total_amount
# 2011-01-01, 12345.00
# ...
# Task 2
# Following up on the same Apache Beam batch job, also do the following 1. Group all transform steps into a single Composite Transform 1. 
# Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam
# Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam
# Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam
#----------------------------#

import json
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions

def parse_row(row):
    parts = row.split(',')
    return {
        'date': parts[0].strip()[:10],      # Extracts YYYY-MM-DD (First 10 characters of date column)
        'amount': float(parts[3].strip())   # Extracts float values (Fourth column)
    }

class TransactionSummary(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Parse' >> beam.Map(parse_row)
            | 'FilterAmount' >> beam.Filter(lambda x: x['amount'] > 20)         # Filter amounts more than 20
            | 'FilterDate' >> beam.Filter(lambda x: x['date'][:4] >= '2010')    # Takes YYYY
            | 'ToKV' >> beam.Map(lambda x: (x['date'], x['amount']))
            | 'SumByDate' >> beam.CombinePerKey(sum)                            # Sum up amounts by dates
        )

if __name__ == '__main__':
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read' >> beam.io.ReadFromText(
                'transactions.csv',
                skip_header_lines=1
            )
            | 'Transform' >> TransactionSummary()
            | 'Format' >> beam.Map(lambda kv: json.dumps({'date': kv[0], 'total_amount': kv[1]}))
            | 'Write' >> beam.io.WriteToText(
                'output/results.jsonl',
                file_name_suffix = '.gz',
                compression_type = beam.io.filesystem.CompressionTypes.GZIP    # Write output to JSON, then compress using .gz                                      
            )
        )