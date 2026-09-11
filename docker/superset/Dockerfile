FROM apache/superset:4.1.1

USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER superset
