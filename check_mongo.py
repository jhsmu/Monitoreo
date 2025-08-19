import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DATABASE_NAME")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    db.command("ping")  # consulta básica
    print("✅ Conexión exitosa a MongoDB Atlas")
    exit(0)
except ConnectionFailure as e:
    print("❌ Error de conexión:", e)
    exit(1)
