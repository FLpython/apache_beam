import argparse
import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.external import ExternalTransform
from apache_beam.io.jdbc import ReadFromJdbc
import typing
from apache_beam.coders import RowCoder


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default=None,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,  # !!!!!!!!!!!!!!!!!!!!
        help='Output file to write results to.')
# This needed when using local expansion service
    # parser.add_argument(
    #     '--expansion_service_port',
    #     dest='expansion_service_port',
    #     required=True,
    #     help='Expansion service port')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # I think this edits the sql query......
    # ExampleRow = typing.NamedTuple('ExampleRow',
    #                                [('id', int), ('name', unicode)])
    # coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with beam.Pipeline(options=pipeline_options) as p:
        result = p | ReadFromJdbc(
            table_name='orders',
            driver_class_name='org.postgresql.Driver',
            jdbc_url='jdbc:postgresql://34.135.220.68:5432/test',
            username='postgres',
            password='admin',
            query='SELECT id FROM orders')
        # expansion_service='localhost:12345')

        result | WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
