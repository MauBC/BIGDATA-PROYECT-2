"""Configuración de conexiones compartida por ETL, scripts, CRUD y dashboard.

Dentro de Docker las variables vienen de docker-compose/.env; desde Windows
(fuera de Docker) se usan los valores por defecto contra localhost.
"""
import os

DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "raw", "data"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "reddit")

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "reddit")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "neo4j12345"))

SUBREDDITS = ["LocalLLaMA", "wallstreetbets", "SkincareAddiction"]
