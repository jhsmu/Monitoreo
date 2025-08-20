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

# Conectar a MongoDB (tu base real, no local)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
loan_col = db["loan"]

# Definir métricas
creditos_totales = Gauge('creditos_totales', 'Total de créditos (limitado a 20)')
creditos_condiciones = Gauge('creditos_condiciones', 'Créditos en mora/pagos (limitado a 20)')

# Definir query de condiciones
query = {
    "$or": [
        {"status": "arrear"},
        {"payment_amount": 0},
        {"amortization.total_amount": 0},
        {"amortization.pending_payment": {"$gt": 0}}
    ]
}

def calcular_metricas():
    # Tomar solo 20 registros desde MongoDB
    sample_docs = list(loan_col.find().limit(20))
    total = len(sample_docs)

    # Validar cuántos cumplen la query
    en_condiciones = sum(1 for doc in sample_docs if (
        doc.get("status") == "arrear"
        or doc.get("payment_amount", 0) == 0
        or doc.get("amortization", {}).get("total_amount", 0) == 0
        or doc.get("amortization", {}).get("pending_payment", 0) > 0
    ))

    # Actualizar métricas
    creditos_totales.set(total)
    creditos_condiciones.set(en_condiciones)

    print(f"[{datetime.now()}] Métricas (20 docs) → total={total}, en_condiciones={en_condiciones}")

if __name__ == "__main__":
    # Exponer métricas en :8000/metrics
    start_http_server(8000)
    print("Servidor de métricas expuesto en http://localhost:8000/metrics")

    while True:
        calcular_metricas()
        time.sleep(60)  # cada 60s
