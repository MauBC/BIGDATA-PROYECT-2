"""Creación de la base MongoDB `reddit` (idempotente: se puede ejecutar varias veces).

Colecciones:
- posts      : un documento por post, con sus comentarios anidados en `comments`.
- etl_runs   : bitácora de cada ejecución diaria del DAG (_id = fecha procesada).
- rechazados : registros descartados en la limpieza, con el motivo (_id = "<tipo>:<id>").
"""
from pymongo import ASCENDING, DESCENDING, TEXT, MongoClient
from pymongo.errors import CollectionInvalid

from etl.config import MONGO_DB, MONGO_URI

COMMENT_SCHEMA = {
    "bsonType": "object",
    "required": ["id", "parent_id", "depth", "created_utc", "body"],
    "properties": {
        "id": {"bsonType": "string"},
        "parent_id": {"bsonType": "string"},
        "depth": {"bsonType": "int", "minimum": 0},
        "author": {"bsonType": ["string", "null"]},
        "created_utc": {"bsonType": "date"},
        "fecha": {"bsonType": "string"},
        "body": {"bsonType": "string"},
        "score": {"bsonType": ["int", "long"]},
        "is_submitter": {"bsonType": ["bool", "null"]},
        "permalink": {"bsonType": ["string", "null"]},
    },
}

POSTS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "subreddit", "created_utc", "fecha", "title", "comments"],
        "properties": {
            "_id": {"bsonType": "string", "description": "id del post (sin prefijo t3_)"},
            "subreddit": {"enum": ["LocalLLaMA", "wallstreetbets", "SkincareAddiction"]},
            "author": {"bsonType": ["string", "null"]},
            "created_utc": {"bsonType": "date"},
            "fecha": {"bsonType": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
            "title": {"bsonType": "string"},
            "selftext": {"bsonType": ["string", "null"]},
            "url": {"bsonType": ["string", "null"]},
            "score": {"bsonType": ["int", "long"]},
            "upvote_ratio": {"bsonType": ["double", "null"]},
            "num_comments": {"bsonType": ["int", "long"]},
            "flair": {"bsonType": ["string", "null"]},
            "is_nsfw": {"bsonType": "bool"},
            "permalink": {"bsonType": ["string", "null"]},
            "comments": {"bsonType": "array", "items": COMMENT_SCHEMA},
        },
    }
}

POSTS_INDEXES = [
    ([("subreddit", ASCENDING), ("created_utc", DESCENDING)], {"name": "subreddit_fecha"}),
    ([("fecha", ASCENDING)], {"name": "fecha"}),
    ([("author", ASCENDING)], {"name": "author"}),
    ([("comments.id", ASCENDING)], {"name": "comments_id"}),
    ([("comments.author", ASCENDING)], {"name": "comments_author"}),
    ([("comments.fecha", ASCENDING)], {"name": "comments_fecha"}),
    ([("title", TEXT), ("selftext", TEXT)], {"name": "texto", "default_language": "english"}),
]


def ensure_collection(db, name, validator=None):
    opts = {"validator": validator, "validationLevel": "moderate", "validationAction": "error"} if validator else {}
    try:
        db.create_collection(name, **opts)
        print(f"  [mongo] colección creada: {name}")
    except CollectionInvalid:
        if validator:
            db.command("collMod", name, **opts)
        print(f"  [mongo] colección ya existía: {name}")


def main():
    with MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) as client:
        db = client[MONGO_DB]
        ensure_collection(db, "posts", POSTS_VALIDATOR)
        ensure_collection(db, "etl_runs")
        ensure_collection(db, "rechazados")

        for keys, opts in POSTS_INDEXES:
            db.posts.create_index(keys, **opts)
        db.rechazados.create_index([("tipo", ASCENDING), ("fecha", ASCENDING)], name="tipo_fecha")
        db.rechazados.create_index([("motivo", ASCENDING)], name="motivo")
        print(f"  [mongo] índices en posts: {sorted(db.posts.index_information())}")


if __name__ == "__main__":
    main()
