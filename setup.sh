docker buildx build --platform linux/amd64 -t iaap-cli-beam:latest --load --no-cache .

docker run --platform linux/amd64 -it --rm --entrypoint /bin/bash iaap-cli-beam:latest