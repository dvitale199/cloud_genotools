import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import subprocess
import argparse

class RunIaapCliDoFn(beam.DoFn):
    def __init__(self, iaap_cli, input_base_path, out_path, bpm, egt):
        """Initialize the DoFn with the necessary parameters.

        Args:
            iaap_cli (str): Path to the iaap-cli executable.
            input_base_path (str): Base GCS path to the input idats.
            out_path (str): GCS path to the output directory.
            bpm (str): GCS path to the bpm file.
            egt (str): GCS path to the egt file.
        """
        self.iaap_cli = iaap_cli
        self.input_base_path = input_base_path
        self.out_path = out_path
        self.bpm = bpm
        self.egt = egt

    def process(self, barcode):
        """Runs iaap-cli command for the given barcode.

        Args:
            barcode (str): The barcode to process.

        Yields:
            str: A message indicating success or failure.
        """
        barcode = barcode.strip()  # remove any leading/trailing whitespace
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

        print(f"Executing command: {' '.join(iaap_command)}")
        # try:
        #     subprocess.run(iaap_command, check=True)
        #     yield f'Barcode {barcode}: Processing completed successfully.'
        # except subprocess.CalledProcessError as e:
        #     error_msg = f'Barcode {barcode}: Error during processing.\n{e}'
        #     yield error_msg

def run_pipeline(known_args, pipeline_args=None):
    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadBarcodes' >> beam.io.ReadFromText(known_args.barcodes_file)
            | 'RunIaapCli' >> beam.ParDo(
                RunIaapCliDoFn(
                    iaap_cli=known_args.iaap_cli,
                    input_base_path=known_args.input_base_path,
                    out_path=known_args.out_path,
                    bpm=known_args.bpm,
                    egt=known_args.egt
                )
            )
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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
    known_args, pipeline_args = parser.parse_known_args()

    run_pipeline(known_args, pipeline_args)