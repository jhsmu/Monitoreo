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
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
loan_col = db["loan"]

# Definir métricas
creditos_totales = Gauge('creditos_totales', 'Total de créditos en la colección loan')
creditos_condiciones = Gauge('creditos_condiciones', 'Créditos que cumplen query de mora/pagos')

# Definir query
query = {
    "$or": [
        {"status": "arrear"},
        {"payment_amount": 0},
        {"amortization.total_amount": 0},
        {"amortization.pending_payment": {"$gt": 0}}
    ]
}

def calcular_metricas():
    if TEST_MODE:
        # MODO PRUEBA → tomar solo 20 docs
        sample_docs = list(loan_col.find().limit(20))
        total = len(sample_docs)
        en_condiciones = sum(1 for doc in sample_docs if (
            doc.get("status") == "arrear"
            or doc.get("payment_amount", 0) == 0
            or doc.get("amortization", {}).get("total_amount", 0) == 0
            or doc.get("amortization", {}).get("pending_payment", 0) > 0
        ))
        print(f"[{datetime.now()}] (PRUEBA) Métricas → total_muestra={total}, en_condiciones={en_condiciones}")
    else:
        # MODO REAL → contar en toda la colección
        total = loan_col.count_documents({})
        en_condiciones = loan_col.count_documents(query)
        print(f"[{datetime.now()}] (REAL) Métricas → total={total}, en_condiciones={en_condiciones}")

    # Actualizar métricas Prometheus
    creditos_totales.set(total)
    creditos_condiciones.set(en_condiciones)

if __name__ == "__main__":
    # Levantar servidor en :8000/metrics
    start_http_server(8000)
    print("Servidor de métricas expuesto en http://localhost:8000/metrics")
    print(f"Modo prueba: {TEST_MODE}")

    while True:
        calcular_metricas()
        time.sleep(60)  # cada 60s
