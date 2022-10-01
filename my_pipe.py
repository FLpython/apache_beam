import argparse
import logging
import re
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
    Duration,
)
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["timestamp"]
        element = (element["userId"], element["click"])
        yield TimestampedValue(element, unix_timestamp)


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
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        events = p | beam.Create(
            [
                {"userId": "Andy", "click": 1, "timestamp": 1603112520},  # Event time: 13:02
                {"userId": "Sam", "click": 1, "timestamp": 1603113240},  # Event time: 13:14
                {"userId": "Andy", "click": 1, "timestamp": 1603115820},  # Event time: 13:57
                {"userId": "Andy", "click": 1, "timestamp": 1603113600},  # Event time: 13:20
            ]
        )
        timestamped_events = events | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())

        windowed_events = timestamped_events | beam.WindowInto(
            Sessions(gap_size=30 * 60),
            trigger=None,
            accumulation_mode=None,
            timestamp_combiner=None,
            allowed_lateness=Duration(seconds=1 * 24 * 60 * 60),  # 1 day
        )

        sum_clicks = windowed_events | beam.CombinePerKey(sum)

        sum_clicks | WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
