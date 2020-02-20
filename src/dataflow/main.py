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
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import (GoogleCloudOptions,
                                                  PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions)
from apache_beam.pvalue import AsDict
from apache_beam.transforms.combiners import Count


class DataIngestion(object):

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        schema_file = os.path.join(dir_path, 'schema', 'price_bill_comp.json')
        with open(schema_file) as f:
            self.price_bill_comp_schema = '{"fields": ' + f.read() + '}'


class print_element(beam.DoFn):

    def process(self, element, label='?????'):
        print("{0} === {1}".format(label, element))
        yield element


class filter_by_key(beam.DoFn):
    """ Filtra as linhas por um id específico
    Uso:  beam.ParDo(filter_by_id(), 'XYZ') """

    def process(self, element, field, id):
        if element[field] == id:
            yield element


class ab_flatener(beam.DoFn):

    def process(self, element):
        ele = []
        for item in element['price_quote']:
            bill = element['bill_of_materials'][0].split(',')
            aaa = [str(b) for b in bill]
            ele = element['price_quote'] + aaa
        print(ele)
        yield ele


class price_quote_to_dict(beam.DoFn):

    def process(self, element):
        item_list = element.split(',')
        yield {
            'tube_assembly_id': item_list[0],
            'supplier': item_list[1],
            'quote_date': item_list[2],
            'annual_usage': item_list[3],
            'min_order_quantity': item_list[4],
            'bracket_pricing': item_list[5],
            'quantity': item_list[6],
            'cost': item_list[7]
        }


class bill_to_dict(beam.DoFn):

    def process(self, element):
        item_list = element.split(',')
        yield {
            'tube_assembly_id': item_list[0],
            'component_id_1': item_list[1],
            'quantity_1': item_list[2],
            'component_id_2': item_list[3],
            'quantity_2': item_list[4],
            'component_id_3': item_list[5],
            'quantity_3': item_list[6],
            'component_id_4': item_list[7],
            'quantity_4': item_list[8],
            'component_id_5': item_list[9],
            'quantity_5': item_list[10],
            'component_id_6': item_list[11],
            'quantity_6': item_list[12],
            'component_id_7': item_list[13],
            'quantity_7': item_list[14],
            'component_id_8': item_list[15],
            'quantity_8': item_list[16]
        }


class comp1_to_dict(beam.DoFn):

    def process(self, element):
        item_list = element.split(',')
        yield {
            'component_id_1': item_list[0],
            'comp1_component_type_id': item_list[1],
            'comp1_type': item_list[2],
            'comp1_connection_type_id': item_list[3],
            'comp1_outside_shape': item_list[4],
            'comp1_base_type': item_list[5],
            'comp1_height_over_tube': item_list[6],
            'comp1_bolt_pattern_long': item_list[7],
            'comp1_bolt_pattern_wide': item_list[8],
            'comp1_groove': item_list[9],
            'comp1_base_diameter': item_list[10],
            'comp1_shoulder_diameter': item_list[11],
            'comp1_unique_feature': item_list[12],
            'comp1_orientation': item_list[13],
            'comp1_weight': item_list[14]
        }


class comp2_to_dict(beam.DoFn):

    def process(self, element):
        item_list = element.split(',')
        yield {
            'component_id_2': item_list[0],
            'comp2_component_type_id': item_list[1],
            'comp2_type': item_list[2],
            'comp2_connection_type_id': item_list[3],
            'comp2_outside_shape': item_list[4],
            'comp2_base_type': item_list[5],
            'comp2_height_over_tube': item_list[6],
            'comp2_bolt_pattern_long': item_list[7],
            'comp2_bolt_pattern_wide': item_list[8],
            'comp2_groove': item_list[9],
            'comp2_base_diameter': item_list[10],
            'comp2_shoulder_diameter': item_list[11],
            'comp2_unique_feature': item_list[12],
            'comp2_orientation': item_list[13],
            'comp2_weight': item_list[14]
        }


class LeftJoin(beam.PTransform):

    def __init__(self, source_data, join_data, source_pipeline_name, join_pipeline_name, common_key):
        self.source_data = source_data
        self.join_data = join_data
        self.join_pipeline_name = join_pipeline_name
        self.source_pipeline_name = source_pipeline_name
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            try:
                return data_dict[common_key], data_dict
            except TypeError:
                return {}, {}

        return ({pipeline_name: pcoll | 'Convert to({0}, object) for {1}'
                 .format(self.common_key, pipeline_name) >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(), self.source_pipeline_name, self.join_pipeline_name)
                )


class UnnestCoGrouped (beam.DoFn):

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError: 
                yield source_dictionary


def run(argv=None):
    """ Método principal """

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_a', dest='input_a', required=False,
                        help='Arquivo de entrada', default='gs://dotz-exam/raw/price_quote.csv')
    parser.add_argument('--input_b', dest='input_b', required=False,
                        help='Arquivo de entrada', default='gs://dotz-exam/raw/bill_of_materials.csv')
    parser.add_argument('--input_c', dest='input_c', required=False,
                        help='Arquivo de entrada', default='gs://dotz-exam/raw/comp_boss.csv')
    parser.add_argument('--output_table', dest='output_table', required=False,
                        help='Saida para BQ', default='price_bill_comp')

    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()
    price_key = 'tube_assembly_id'
    bill_key = 'tube_assembly_id'

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema = parse_table_schema_from_json(
        data_ingestion.price_bill_comp_schema)

    price_data = (p
                  | 'price quote - Read text file' >> beam.io.ReadFromText(known_args.input_a, skip_header_lines=1)
                  | 'price quote - Convert to dict' >> beam.ParDo(price_quote_to_dict()))
    # | 'price quote - Filter by id' >> beam.ParDo(filter_by_key(), 'tube_assembly_id', 'TA-11583'))

    bill_data = (p
                 | 'bill - Read text file' >> beam.io.ReadFromText(known_args.input_b, skip_header_lines=1)
                 | 'bill - Convert to dict' >> beam.ParDo(bill_to_dict()))
    # | 'bill - Filter by id' >> beam.ParDo(filter_by_key(), 'tube_assembly_id', 'TA-11583'))

    price_bill_data = ({'price': price_data, 'bill': bill_data}
                       | 'Left join {0} and {1} on {2}'.format('price', 'bill', 'tube_assembly_id')
                       >> LeftJoin(price_data, bill_data, 'price', 'bill', 'tube_assembly_id'))

    comp_data1 = (p
                  | 'comp1 - Read text file' >> beam.io.ReadFromText(known_args.input_c, skip_header_lines=1)
                  | 'comp1 - Convert to dict' >> beam.ParDo(comp1_to_dict()))

    # comp_data2 = (p
    #               | 'comp2 - Read text file' >> beam.io.ReadFromText(known_args.input_c, skip_header_lines=1)
    #               | 'comp2 - Convert to dict' >> beam.ParDo(comp2_to_dict()))

    price_bill_comp_data1 = ({'price_bill': price_bill_data, 'comp1': comp_data1}
                             | 'Left join {0} and {1} on {2}'.format('price_bill', 'comp1', 'component_id_1')
                             >> LeftJoin(price_data, bill_data, 'price_bill', 'comp1', 'component_id_1')
                             | 'Comp1 - Salvar' >> beam.io.WriteToText('./tmp/', 'comp1')
                             | 'Salvar no GCS' >> beam.io.WriteToText(
                                 file_path_prefix='gs://dotz-exam/work/',
                                 file_name_suffix='.json',
                                 append_trailing_newlines=True))

    # TODO: Fazer a junção das demais informações sobre componentes

    # price_bill_comp_data2 = ({'price_bill_comp_data1': price_bill_comp_data1, 'comp2': comp_data2}
    #                          | 'Left join {0} and {1} on {2}'.format('price_bill_comp_data1', 'comp2', 'component_id_2')
    #                          >> LeftJoin(price_bill_comp_data1, comp_data2, 'price_bill_comp_data1', 'comp2', 'component_id_2')
    #                          | 'Comp2 - Salvar' >> beam.io.WriteToText('./tmp/', 'comp2')
    #                          )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
