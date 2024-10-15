FROM apache/beam_python3.12_sdk:2.60.0
FROM python:3.12

WORKDIR /app

ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ENTRYPOINT [ "/opt/apache/beam/boot" ]