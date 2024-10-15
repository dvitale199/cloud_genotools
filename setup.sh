docker buildx build --platform linux/amd64 -t iaap-cli-beam:latest --load --no-cache .

# for local testing
docker run --platform linux/amd64 -it -v $(pwd):/app --rm --entrypoint /bin/bash iaap-cli-beam:latest

python /app/pipeline/idat_to_bed_beam.py \
  --barcodes_file /app/data/idats.txt \
  --iaap_cli /app/executables/iaap-cli/iaap-cli \
  --input_base_path gs://my-bucket/idats \
  --out_path gs://my-bucket/output \
  --bpm gs://my-bucket/resources/my_file.bpm \
  --egt gs://my-bucket/resources/my_file.egt
#   --runner DirectRunner