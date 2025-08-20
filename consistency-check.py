import os
import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
from prometheus_client import start_http_server, Gauge
import threading
import time

load_dotenv()

# Configuración de la URI de MongoDB
MONGODB_URI = os.getenv('MONGODB_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
COLLECTION_NAME = 'loan'

STOP_ID = os.getenv('STOP_ID')
YOYO_ID = os.getenv('YOYO_ID')

if not STOP_ID or not YOYO_ID: 
    raise Exception("Configura los IDs en las variables de entorno")

# Directorio y archivo de backup
output_dir = "backups"
os.makedirs(output_dir, exist_ok=True)

# Campos que deben ser int
int_keys = [
    "principal", "total_amount", "principal_payment_amount", "interest_amount", "taxes",
    "days_in_arrear", "pending_payment", "arrear_interest_amount", "pending_principal_payment_amount",
    "pending_interest_amount", "pending_interest_taxes_amount", "pending_arrear_interest_amount",
    "pending_guarantee_amount", "pending_guarantee_taxes_amount", "pending_other_expenses_amount",
    "period_days", "interest_taxes_amount", "guarantee_amount", "guarantee_taxes_amount",
    "other_expenses_amount", "arrear_interest_paid", "arrear_interest_taxes_amount",
    "pending_arrear_interest_taxes_amount",
]

# ----- Métricas Prometheus -----
loan_documents_count_gauge = Gauge('mongo_loan_documents_count', 'Cantidad de préstamos encontrados')
updated_loans_gauge = Gauge('mongo_updated_loans_count', 'Cantidad de préstamos con amortization actualizada')
validated_users_gauge = Gauge('mongo_validated_users_count', 'Cantidad de usuarios validados')
updated_users_gauge = Gauge('mongo_updated_users_count', 'Cantidad de usuarios actualizados')

# ----- Funciones existentes -----
def connect_to_mongodb(uri):
    try:
        client = MongoClient(uri)
        client.admin.command('ping')
        print("✅ Conexión exitosa a MongoDB Atlas")
        return client
    except Exception as e:
        print(f"❌ Error al conectar a MongoDB: {e}")
        return None

def get_loan_documents(db):
    try:
        query = {
            "financial_entity_id": {"$in": [STOP_ID, YOYO_ID]},
            "status": "paid",
            "amortization": {"$elemMatch": {"days_in_arrear": {"$gt": 0}}}
        }
        loan_collection = db.loan
        results = list(loan_collection.find(query))
        return results
    except Exception as e:
        print(f"❌ Error al consultar la colección loan: {e}")
        return []

def save_to_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"❌ Error al guardar el archivo JSON: {e}")
        return False

def update_amortization_arrears(db, loan_documents):
    try:
        loan_collection = db.loan
        updated_loans = []
        for i, loan_doc in enumerate(loan_documents, 1):
            loan_id = loan_doc.get("_id")
            amortization = loan_doc.get("amortization", [])
            if not amortization:
                continue
            arrear_elements = []
            update_array = []
            for j, element in enumerate(amortization):
                days_in_arrear = int(element.get("days_in_arrear", 0))
                if days_in_arrear > 0:
                    arrear_elements.append({"index": j, "days_in_arrear": days_in_arrear})
                    updated_element = element.copy()
                    updated_element["days_in_arrear"] = 0
                    update_array.append(updated_element)
                else:
                    update_array.append(element)
            if not arrear_elements:
                continue
            try:
                update_result = loan_collection.update_one(
                    {"_id": loan_id},
                    {"$set": {"amortization": update_array}}
                )
                if update_result.modified_count > 0:
                    updated_loans.append({
                        "loan_id": str(loan_id),
                        "elements_updated": len(arrear_elements),
                        "arrear_elements": arrear_elements
                    })
            except Exception as update_error:
                print(f"❌ Error al actualizar préstamo {i}: {update_error}")
        return updated_loans
    except Exception as e:
        print(f"❌ Error al actualizar amortization: {e}")
        return []

def validate_user_status(db, loan_documents):
    try:
        user_collection = db.user
        loan_collection = db.loan
        validation_results = []
        updated_users = []
        unique_user_ids = {loan.get("user_id") for loan in loan_documents if loan.get("user_id")}
        for user_id in unique_user_ids:
            user_doc = user_collection.find_one({"_id": user_id})
            if not user_doc:
                validation_results.append({"user_id": str(user_id), "user_status": "No encontrado",
                                           "user_found": False, "loans_found": 0, "status_updated": False})
                continue
            user_status = user_doc.get("status", "No especificado")
            if user_status == "arrear":
                user_loans = list(loan_collection.find({"user_id": user_id}))
                arrear_loans = [loan for loan in user_loans if loan.get("status") == "arrear"]
                should_update = len(user_loans) == 1 or len(arrear_loans) == 0
                if should_update:
                    user_collection.update_one({"_id": user_id}, {"$set": {"status": "active"}})
                    updated_users.append({"user_id": str(user_id), "old_status": "arrear", "new_status": "active"})
                validation_results.append({"user_id": str(user_id), "user_status": user_status,
                                           "user_found": True, "loans_found": len(user_loans),
                                           "arrear_loans": len(arrear_loans), "status_updated": should_update})
            else:
                validation_results.append({"user_id": str(user_id), "user_status": user_status,
                                           "user_found": True, "loans_found": 0, "status_updated": False})
        return validation_results, updated_users
    except Exception as e:
        print(f"❌ Error al validar usuarios: {e}")
        return [], []

# ----- Función de métricas para Prometheus -----
def update_metrics_periodically(db):
    while True:
        loan_docs = get_loan_documents(db)
        amortization_updates = update_amortization_arrears(db, loan_docs)
        validation_results, updated_users = validate_user_status(db, loan_docs)

        # Actualizar métricas Prometheus
        loan_documents_count_gauge.set(len(loan_docs))
        updated_loans_gauge.set(len(amortization_updates))
        validated_users_gauge.set(len(validation_results))
        updated_users_gauge.set(len(updated_users))

        time.sleep(15)  # Actualización cada 15 segundos

# ----- Función principal -----
def main():
    client = connect_to_mongodb(MONGODB_URI)
    if not client:
        return
    db = client[DATABASE_NAME]

    # Iniciar servidor Prometheus en puerto 8000
    start_http_server(8000)
    print("🚀 Servidor Prometheus escuchando en http://localhost:8000")

    # Ejecutar actualización de métricas en hilo aparte
    metrics_thread = threading.Thread(target=update_metrics_periodically, args=(db,))
    metrics_thread.daemon = True
    metrics_thread.start()

    # Mantener script vivo
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("🔌 Script detenido")
    finally:
        client.close()

if __name__ == "__main__":
    main()
