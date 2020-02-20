#!/usr/bin/python
# -*- coding: UTF-8 -*-

from __future__ import absolute_import

import argparse
import csv
import datetime
import logging
import os
import re

import apache_beam as beam
import apache_beam.io.gcp.bigquery_tools
from apache_beam.io.gcp import bigquery
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions)
from apache_beam.pvalue import AsDict
from apache_beam.transforms.combiners import Count
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


class DataIngestion(object):

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        schema_file = os.path.join(dir_path, 'schema', 'price_quote.json')
        with open(schema_file) as f:
            self.price_quote_schema = '{"fields": ' + f.read() + '}'


class print_element(beam.DoFn):

    def process(self, element, label='?????'):
        print("{0} >> {1}".format(label, element))
        yield element


class filter_by_key(beam.DoFn):
    """ Filtra as linhas por um id específico
    Uso:  beam.ParDo(filter_by_id(), 'XYZ') """

    def process(self, element, tube_assembly_id):
        if element[0] == tube_assembly_id:
            yield element


class ab_flatener(beam.DoFn):

    def process(self, element):
        
        yield element


def run(argv=None):
    """ Método principal """

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_a',
                        dest='input_a',
                        required=False,
                        help='Arquivo de entrada',
                        default='gs://dotz-exam/raw/price_quote.csv')

    parser.add_argument('--input_b',
                        dest='input_b',
                        required=False,
                        help='Arquivo de entrada',
                        default='gs://dotz-exam/raw/bill_of_materials.csv')

    parser.add_argument('--input_c',
                        dest='input_c',
                        required=False,
                        help='Arquivo de entrada',
                        default='gs://dotz-exam/raw/comp_boss.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Saida para BQ',
                        default='dotzexam.price_quote')

    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema = parse_table_schema_from_json(data_ingestion.price_quote_schema)

    # argv = None  # if None, uses sys.argv
    # pipeline_options = PipelineOptions(argv)
    # with beam.Pipeline(options=pipeline_options) as pipeline:

    price_quote = (
        p
        | 'Leitura' >> beam.io.ReadFromText(known_args.input_a, skip_header_lines=1)
        | 'Cria chave' >> beam.Map(lambda row: (row.split(',')[0], row))
        | 'Filter by id - price_quote' >> beam.ParDo(filter_by_key(), 'TA-11583')
        | 'Print price_quote' >> beam.ParDo(print_element(), 'price')
    )

    bill_of_materials = (
        p
        | 'Leitura 2' >> beam.io.ReadFromText(known_args.input_b, skip_header_lines=1)
        | 'Cria chave 2' >> beam.Map(lambda row: (row.split(',')[0], row))
        | 'Filter by id - bill_of_materials' >> beam.ParDo(filter_by_key(), 'TA-11583')
        | 'Print bill_of_materials' >> beam.ParDo(print_element(), '_bill')
    )

    comp_boss = (
        p
        | 'Leitura 3' >> beam.io.ReadFromText(known_args.input_b, skip_header_lines=1)
        | 'Cria chave 3' >> beam.Map(lambda row: (row.split(',')[0], row))
        | 'Filter by id - comp_boss' >> beam.ParDo(filter_by_key(), 'C-1845')
        | 'Print comp_boss' >> beam.ParDo(print_element(), '_comp')
    )

    join_ab = ({'price_quote': price_quote, 'bill_of_materials': bill_of_materials}
               | 'Agrupar por chave' >> beam.CoGroupByKey()
               | 'Imprimir A e B' >> beam.ParDo(print_element(), 'full1')
               | 'Organizar elementos' >> beam.ParDo(ab_flatener())
               | 'Imprimir A e B depois de organizar' >> beam.ParDo(print_element(), 'full2')
               # | 'Salvar' >> beam.io.WriteToText('./tmp/')
               )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARN)
    run()
