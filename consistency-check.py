# metrics_server.py
import os
import time
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from prometheus_client import start_http_server, Gauge

load_dotenv()

# Configuración de MongoDB
MONGODB_URI = os.getenv('MONGODB_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
STOP_ID = os.getenv('STOP_ID')
YOYO_ID = os.getenv('YOYO_ID')

# Métricas Prometheus
loans_total = Gauge('loans_total', 'Total number of loans processed')
amortization_updated = Gauge('amortization_updated', 'Number of loans with amortization updated')
users_validated = Gauge('users_validated', 'Number of users validated')
users_updated = Gauge('users_updated', 'Number of users updated')

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def connect_to_mongodb(uri):
    log("🔗 Conectando a MongoDB...")
    try:
        client = MongoClient(uri)
        client.admin.command('ping')
        log("✅ Conexión exitosa a MongoDB")
        return client
    except Exception as e:
        log(f"❌ Error conectando a MongoDB: {e}")
        return None

def update_metrics(db):
    """Consulta MongoDB y actualiza las métricas Prometheus"""
    loan_col = db.loan
    user_col = db.user

    # Loans con amortization > 0
    loans = list(loan_col.find({
        "financial_entity_id": {"$in": [STOP_ID, YOYO_ID]},
        "status": "paid",
        "amortization": {"$elemMatch": {"days_in_arrear": {"$gt": 0}}}
    }))
    loans_total.set(len(loans))

    # Amortization actualizada (days_in_arrear > 0)
    amort_count = sum(
        sum(1 for a in loan.get("amortization", []) if a.get("days_in_arrear", 0) > 0)
        for loan in loans
    )
    amortization_updated.set(amort_count)

    # Usuarios únicos validados
    user_ids = set(loan.get("user_id") for loan in loans if loan.get("user_id"))
    users_validated.set(len(user_ids))

    # Usuarios con status actualizado a active
    updated_users_count = user_col.count_documents({"status": "active"})
    users_updated.set(updated_users_count)

    log(f"📊 Métricas actualizadas: loans={len(loans)}, amortization={amort_count}, "
        f"users_validated={len(user_ids)}, users_updated={updated_users_count}")

def main():
    log("🚀 Iniciando servidor de métricas Prometheus en el puerto 8000")
    start_http_server(8000)

    client = connect_to_mongodb(MONGODB_URI)
    if not client:
        log("❌ No se pudo conectar a MongoDB. Saliendo...")
        return

    db = client[DATABASE_NAME]

    # Bucle infinito para actualizar métricas cada 60 segundos
    while True:
        try:
            update_metrics(db)
        except Exception as e:
            log(f"❌ Error actualizando métricas: {e}")
        time.sleep(60)

if __name__ == "__main__":
    main()
