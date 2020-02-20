from __future__ import absolute_import

import argparse
import logging
import re
import datetime

import apache_beam as beam
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions)
from apache_beam.transforms.combiners import Count

#input_filename = 'gs://dotz-exam/raw/price_quote.csv'
input_filename = './data/price_quote.csv'

options = PipelineOptions()
gcloud_options = options.view_as(GoogleCloudOptions)
gcloud_options.job_name = 'test-job'
#gcloud_options.project = ''
#gcloud_options.temp_location = ''
#gcloud_options.staging_location = ''

# Local runner
options.view_as(StandardOptions).runner = 'direct'
# Dataflow runner
#options.view_as(StandardOptions).runner = 'dataflow'


class Split(beam.DoFn):
    def process(self, element, *args, **kwargs):

        tube_assembly_id, supplier, quote_date, annual_usage, min_order_quantity, bracket_pricing, quantity, cost = element.split(
            ",")

        return [{
            'tube_assembly_id': tube_assembly_id,
            'supplier': supplier,
            'quote_date': quote_date,
            'annual_usage': annual_usage,
            'min_order_quantity': min_order_quantity,
            'bracket_pricing': bracket_pricing,
            'quantity': quantity,
            'cost': cost
        }]


class WriteToCSV(beam.DoFn):
    def process(self, element, *args, **kwargs):
        #print(element)
        result = ["aaaaaaaaaaaa"]
        return result


with beam.Pipeline(options=options) as p:
    rows = (
        p |
        ReadFromText(input_filename) |
        beam.ParDo(Split()) |
        #beam.ParDo(WriteToCSV())
        beam.io.WriteToText('./output/{}'.format(datetime.datetime.now().time()))
    )
