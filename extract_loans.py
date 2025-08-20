import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from prometheus_client import start_http_server, Gauge
import time

# Cargar variables de entorno (.env)
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DATABASE_NAME")

# Conectar a MongoDB Atlas
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
loan_col = db["loan"]

# Definir métricas
creditos_totales = Gauge('creditos_totales', 'Total de créditos en la colección loan')
creditos_inconsistentes = Gauge('creditos_inconsistentes', 'Créditos que cumplen query de mora/pagos')

# Definir query para inconsistencias
query = {
    "$or": [
        {"status": "arrear"},
        {"payment_amount": 0},
        {"amortization.total_amount": 0},
        {"amortization.pending_payment": {"$gt": 0}}
    ]
}

def calcular_metricas():
    # Solo extraer los primeros 20 documentos para prueba
    sample_docs = loan_col.find(query).limit(20)
    
    total = loan_col.count_documents({})
    inconsistentes = loan_col.count_documents(query)
    
    creditos_totales.set(total)
    creditos_inconsistentes.set(inconsistentes)
    
    print(f"[{datetime.now()}] Métricas → total={total}, inconsistentes={inconsistentes}")
    print("Ejemplo de documentos (máx 20):")
    for doc in sample_docs:
        print({
            "_id": str(doc.get("_id")),
            "user_id": doc.get("user_id"),
            "status": doc.get("status"),
            "payment_amount": doc.get("payment_amount"),
            "total_amount": sum(a.get("total_amount", 0) for a in doc.get("amortization", [])),
            "pending_payment": sum(a.get("pending_payment", 0) for a in doc.get("amortization", []))
        })

if __name__ == "__main__":
    # Servidor Prometheus en puerto 8000
    start_http_server(8000)
    print("Servidor de métricas disponible en http://localhost:8000/metrics")
    
    while True:
        calcular_metricas()
        time.sleep(300)  # cada 5 minutos
