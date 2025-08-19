import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Leer MONGO_URI desde variables de entorno
MONGO_URI = os.getenv("MONGO_URI")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")  # comando para validar conexión
    print("✅ Conexión exitosa a MongoDB Atlas")
except ConnectionFailure as e:
    print("❌ Error de conexión a MongoDB Atlas:", e)
    exit(1)
