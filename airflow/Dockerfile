FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install all providers and dependencies at build time
# so every container (webserver, scheduler) has them available
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
