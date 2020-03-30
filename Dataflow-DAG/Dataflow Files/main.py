
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest="input", required=True)
    parser.add_argument("--output", dest="output", required=True)
    app_args, pipeline_args = parser. parse_known_args()

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        lines = p | 'LOAD' >> beam.io.ReadFromText(app_args.input)
        lines | "WRITE" >> beam.io.WriteToText(app_args.output)


if __name__ == '__main__':
    run()
