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
creditos_condiciones = Gauge('creditos_condiciones', 'Créditos que cumplen query de mora/pagos (prueba con 20 docs)')

# Función para validar condiciones
def cumple_condiciones(doc):
    # Caso 1: status
    if doc.get("status") == "arrear":
        return True
    
    # Caso 2: payment_amount
    if doc.get("payment_amount", 0) == 0:
        return True

    # Caso 3: amortization puede ser dict o list
    amort = doc.get("amortization", {})
    if isinstance(amort, dict):
        if amort.get("total_amount", 0) == 0 or amort.get("pending_payment", 0) > 0:
            return True
    elif isinstance(amort, list):
        for a in amort:
            if a.get("total_amount", 0) == 0 or a.get("pending_payment", 0) > 0:
                return True

    return False

# Función para calcular métricas
def calcular_metricas():
    total = loan_col.count_documents({})

    # Tomar solo 20 documentos para prueba
    sample_docs = loan_col.find({}, {"status": 1, "payment_amount": 1, "amortization": 1}).limit(20)
    en_condiciones = sum(1 for doc in sample_docs if cumple_condiciones(doc))

    # Actualizar métricas
    creditos_totales.set(total)
    creditos_condiciones.set(en_condiciones)

    print(f"[{datetime.now()}] Métricas → total={total}, en_condiciones(20 docs)={en_condiciones}")

if __name__ == "__main__":
    # Levantar servidor en :8000/metrics
    start_http_server(8000)
    print("Servidor de métricas expuesto en http://localhost:8000/metrics")

    while True:
        calcular_metricas()
        time.sleep(60)  # cada 60s
