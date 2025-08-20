import os
import csv
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno (.env)
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DATABASE_NAME")

# Conectar a MongoDB Atlas
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
loan_col = db["loan"]

# Definir query para detectar inconsistencias
query = {
    "$or": [
        {"status": "arrear"},
        {"payment_amount": 0},
        {"amortization.total_amount": 0},
        {"amortization.pending_payment": {"$gt": 0}}
    ]
}

# Ejecutar consulta
results = list(loan_col.find(query))

# 📂 Ruta fija para exportar (montada desde docker-compose)
EXPORT_DIR = "/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)  # por si no existe dentro del contenedor

filename = os.path.join(
    EXPORT_DIR,
    f"report_loans_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
)

with open(filename, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    
    # Escribir encabezados
    writer.writerow([
        "loan_id", "user_id", "status", 
        "payment_amount", "total_amount", "pending_payment"
    ])
    
    # Escribir datos
    for doc in results:
        writer.writerow([
            str(doc.get("_id")),
            doc.get("user_id"),
            doc.get("status"),
            doc.get("payment_amount"),
            doc.get("amortization", {}).get("total_amount"),
            doc.get("amortization", {}).get("pending_payment")
        ])

print(f"✅ Reporte generado en: {filename}")
