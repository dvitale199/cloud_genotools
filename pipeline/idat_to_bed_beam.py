import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import subprocess
import logging

class IaapOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--barcodes_file',
            required=True,
            help='GCS path to the barcodes text file.'
        )
        parser.add_argument(
            '--iaap_cli',
            required=True,
            help='Path to the iaap-cli executable.'
        )
        parser.add_argument(
            '--input_base_path',
            required=True,
            help='Base GCS path to the input idats.'
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

class RunIaapCliDoFn(beam.DoFn):
    def __init__(self, iaap_cli, input_base_path, out_path, bpm, egt):
        self.iaap_cli = iaap_cli
        self.input_base_path = input_base_path
        self.out_path = out_path
        self.bpm = bpm
        self.egt = egt

    def process(self, barcode):
        barcode = barcode.strip()
        input_path = f'{self.input_base_path}/{barcode}'
        
        iaap_command = [
            self.iaap_cli,
            'gencall',
            self.bpm,
            self.egt,
            self.out_path,
            '-f', barcode,
            '-p',
            '-t', '4'
        ]

        logging.info(f"Executing command: {' '.join(iaap_command)}")
        print(iaap_command)
        # try:
        #     subprocess.run(iaap_command, check=True)
        #     yield f'Barcode {barcode}: Processing completed successfully.'
        # except subprocess.CalledProcessError as e:
        #     error_msg = f'Barcode {barcode}: Error during processing.\n{e}'
        #     logging.error(error_msg)
        #     yield error_msg

def run_pipeline(pipeline_options):
    iaap_options = pipeline_options.view_as(IaapOptions)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'ReadBarcodes' >> beam.io.ReadFromText(iaap_options.barcodes_file)
            | 'RunIaapCli' >> beam.ParDo(
                RunIaapCliDoFn(
                    iaap_cli=iaap_options.iaap_cli,
                    input_base_path=iaap_options.input_base_path,
                    out_path=iaap_options.out_path,
                    bpm=iaap_options.bpm,
                    egt=iaap_options.egt
                )
            )
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    iaap_options = pipeline_options.view_as(IaapOptions)

    run_pipeline(pipeline_options)