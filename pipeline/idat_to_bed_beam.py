import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import subprocess
import logging
import os

class IaapOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--barcodes_file',
            required=True,
            help='GCS path to the barcodes text file.'
        )
        parser.add_argument(
            '--out_path',
            required=True,
            help='GCS path to the output directory.'
        )
        parser.add_argument(
            '--bpm',
            required=True,
            help='GCS path to the bpm file.'
        )
        parser.add_argument(
            '--egt',
            required=True,
            help='GCS path to the egt file.'
        )
        parser.add_argument(
        '--threads',
        required=False,
        default='2',
        help='Base GCS path to the input idats.'
    )

class RunIaapCliDoFn(beam.DoFn):
    def __init__(self, out_path, bpm, egt, threads):
        
        self.out_path = out_path
        self.bpm = bpm
        self.egt = egt
        self.threads = threads

    def process(self, barcode_path):
        iaap_cli = '/app/executables/iaap-cli-linux-x64-1.1.0-sha.80d7e5b3d9c1fdfc2e99b472a90652fd3848bbc7/iaap-cli/iaap-cli'
        barcode = barcode_path.strip().split('/')[-1]
        out_barcode_path =  f'{self.out_path}/{barcode}'
             
        iaap_command = [
            iaap_cli,
            'gencall',
            self.bpm,
            self.egt,
            out_barcode_path,
            '-f', barcode_path,
            '-p',
            '-t', self.threads
        ]

        logging.info(f"Executing command: {' '.join(iaap_command)}")
        try:
            subprocess.run(iaap_command, check=True)
            yield f'Barcode {barcode}: Processing completed successfully.'
        except subprocess.CalledProcessError as e:
            error_msg = f'Barcode {barcode}: Error during processing.\n{e}'
            logging.error(error_msg)
            yield error_msg

def run_pipeline(pipeline_options):
    iaap_options = pipeline_options.view_as(IaapOptions)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'ReadBarcodes' >> beam.io.ReadFromText(iaap_options.barcodes_file)
            | 'RunIaapCli' >> beam.ParDo(
                RunIaapCliDoFn(
                    out_path=iaap_options.out_path,
                    bpm=iaap_options.bpm,
                    egt=iaap_options.egt,
                    threads=iaap_options.threads
                )
            )
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    iaap_options = pipeline_options.view_as(IaapOptions)

    run_pipeline(pipeline_options)