import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import subprocess
import argparse

class RunIaapCliDoFn(beam.DoFn):
    def process(self, barcode):
        """Runs iaap-cli command for the given barcode.

        Args:
            barcode (str): The barcode to process.

        Yields:
            str: A message indicating success or failure.
        """
        barcode = barcode.strip()  # Remove any leading/trailing whitespace
        iaap_cli = '/app/executables/iaap-cli/iaap-cli'  # Adjusted path to iaap-cli executable
        input_path = f'gs://path/to/idats/{barcode}'
        out_path = f'gs://path/to/output/'
        bpm = f'gs://path/to/bpm'
        egt = f'gs://path/to/egt'

        iaap_command = [
            iaap_cli,
            'gencall',
            bpm,
            egt,
            out_path,
            '-f', barcode,
            '-p',
            '-t', '4'
        ]

        print(f"Executing command: {' '.join(iaap_command)}")
        try:
            subprocess.run(iaap_command, check=True)
            yield f'Barcode {barcode}: Processing completed successfully.'
        except subprocess.CalledProcessError as e:
            error_msg = f'Barcode {barcode}: Error during processing.\n{e}'
            yield error_msg

def run_pipeline(barcodes_file, pipeline_args=None):
    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadBarcodes' >> beam.io.ReadFromText(barcodes_file)
            | 'RunIaapCli' >> beam.ParDo(RunIaapCliDoFn())
            | 'PrintResults' >> beam.Map(print)
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--barcodes_file',
        required=True,
        help='GCS path to the barcodes text file.'
    )
    known_args, pipeline_args = parser.parse_known_args()

    run_pipeline(known_args.barcodes_file, pipeline_args)