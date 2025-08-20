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
    total = loan_col.count_documents({})
    en_condiciones = loan_col.count_documents(query)

    # Actualizar métricas
    creditos_totales.set(total)
    creditos_condiciones.set(en_condiciones)

    print(f"[{datetime.now()}] Métricas → total={total}, en_condiciones={en_condiciones}")

if __name__ == "__main__":
    # Levantar servidor en :8000/metrics
    start_http_server(8000)
    print("Servidor de métricas expuesto en http://localhost:8000/metrics")

    while True:
        calcular_metricas()
        time.sleep(60)  # cada 60s
