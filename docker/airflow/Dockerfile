FROM apache/airflow:2.10.5-python3.11

# Drivers de las 3 bases (pandas y pyarrow ya vienen en la imagen de Airflow).
# Se respetan las constraints oficiales para no romper dependencias de Airflow.
RUN pip install --no-cache-dir \
      "pymongo" "cassandra-driver" "neo4j" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
