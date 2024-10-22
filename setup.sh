# for local testing
docker buildx build --platform linux/amd64 -t iaap-cli-beam:latest --load --no-cache .
docker run --platform linux/amd64 -it -v $(pwd):/app --rm --entrypoint /bin/bash iaap-cli-beam:latest

python /app/pipeline/idat_to_bed_beam.py \
  --barcodes_file /app/data/idats.txt \
  --out_path /app/data/ped \
  --bpm /app/data/NeuroBooster_20042459_A2.bpm \
  --egt /app/data/recluster_09092022.egt \
  --threads 2 \
  --runner DirectRunner

# on dataflow
docker buildx build --platform linux/amd64 -t europe-west4-docker.pkg.dev/gp2-release-terra/genotools/iaap-cli-beam:latest --load --no-cache .
docker push europe-west4-docker.pkg.dev/gp2-release-terra/genotools/iaap-cli-beam:latest
# another local test after push
docker run --platform linux/amd64 -it -v $(pwd):/app --rm --entrypoint /bin/bash europe-west4-docker.pkg.dev/gp2-release-terra/genotools/iaap-cli-beam:latest

python pipeline/idat_to_bed_beam.py \
  --barcodes_file gs://gp2_dataflow/idats.txt \
  --out_path gs://gp2_dataflow/ped \
  --bpm gs://gp2_dataflow/utils/NeuroBooster_20042459_A2.bpm \
  --egt gs://gp2_dataflow/utils/recluster_09092022.egt \
  --threads 4 \
  --runner DataflowRunner \
  --project gp2-release-terra \
  --region europe-west4 \
  --temp_location gs://gp2_dataflow/temp \
  --job_name idat-test \
  --worker_machine_type n1-standard-4 \
  --sdk_container_image europe-west4-docker.pkg.dev/your-project-id/genotools/iaap-cli-beam:latest \
  --num_workers 3

    # --staging_location gs://my-bucket/staging \