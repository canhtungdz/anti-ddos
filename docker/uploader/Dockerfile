# FROM python:3.10-slim

# RUN pip install elasticsearch

# WORKDIR /app
# COPY upload_to_es.py /app/

# CMD ["python", "upload_to_es.py"]

FROM python:3.10-slim

WORKDIR /app

COPY upload_to_es.py /app/

RUN pip install --upgrade pip && pip install elasticsearch==8.12.1

CMD ["python", "upload_to_es.py"]