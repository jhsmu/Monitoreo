import os
import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
from prometheus_client import start_http_server, Gauge
import threading
import time

load_dotenv()

# Configuración
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

int_keys = [
    "principal", "total_amount", "principal_payment_amount", "interest_amount",
    "taxes", "days_in_arrear", "pending_payment", "arrear_interest_amount",
    "pending_principal_payment_amount", "pending_interest_amount",
    "pending_interest_taxes_amount", "pending_arrear_interest_amount",
    "pending_guarantee_amount", "pending_guarantee_taxes_amount",
    "pending_other_expenses_amount", "period_days", "interest_taxes_amount",
    "guarantee_amount", "guarantee_taxes_amount", "other_expenses_amount",
    "arrear_interest_paid", "arrear_interest_taxes_amount",
    "pending_arrear_interest_taxes_amount",
]

# Métricas para Prometheus
loans_total = Gauge('loans_total', 'Total number of loans processed')
users_updated = Gauge('users_updated', 'Number of users updated')
amortization_updated = Gauge('amortization_updated', 'Number of loans with amortization updated')

def log(msg):
    """Función simple para imprimir con timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def start_metrics_server(port=8000):
    """Arranca servidor para Prometheus en un hilo separado"""
    def run_server():
        log(f"🌐 Iniciando servidor de métricas Prometheus en puerto {port}")
        start_http_server(port)
        while True:
            time.sleep(1)  # Mantener hilo vivo
    t = threading.Thread(target=run_server, daemon=True)
    t.start()

def connect_to_mongodb(uri):
    log("🔗 Intentando conexión a MongoDB...")
    try:
        client = MongoClient(uri)
        client.admin.command('ping')
        log("✅ Conexión exitosa a MongoDB Atlas")
        return client
    except Exception as e:
        log(f"❌ Error al conectar a MongoDB: {e}")
        return None

def get_loan_documents(db):
    log("📋 Ejecutando consulta a la colección loan...")
    try:
        query = {
            "financial_entity_id": {"$in": [STOP_ID, YOYO_ID]},
            "status": "paid",
            "amortization": {"$elemMatch": {"days_in_arrear": {"$gt": 0}}}
        }
        loan_collection = db.loan
        results = list(loan_collection.find(query))
        log(f"📊 Documentos obtenidos: {len(results)}")
        loans_total.set(len(results))
        return results
    except Exception as e:
        log(f"❌ Error al consultar la colección loan: {e}")
        loans_total.set(0)
        return []

def save_to_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log(f"📄 Archivo JSON guardado: {filename}")
        return True
    except Exception as e:
        log(f"❌ Error al guardar el archivo JSON: {e}")
        return False

def update_amortization_arrears(db, loan_documents):
    log(f"🔄 Actualizando amortization para {len(loan_documents)} préstamos...")
    loan_collection = db.loan
    updated_loans = []

    for i, loan_doc in enumerate(loan_documents, 1):
        loan_id = loan_doc.get("_id")
        log(f"🔍 Procesando préstamo {i}: ID={loan_id}")

        amortization = loan_doc.get("amortization", [])
        if not amortization:
            log(f"⚠️ Préstamo {i}: No tiene amortization")
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

            # Validación de tipos
            type_check = all([isinstance(element.get(key, 0), int) for key in int_keys])
            if not type_check:
                log(f"⚠️ Crédito {loan_id} tiene campos flotantes en amortization {element.get('id')}")

        if not arrear_elements:
            log(f"ℹ️ Préstamo {i}: No tiene elementos con days_in_arrear > 0")
            continue

        # Actualizar en MongoDB
        try:
            result = loan_collection.update_one({"_id": loan_id}, {"$set": {"amortization": update_array}})
            if result.modified_count > 0:
                log(f"✅ Préstamo {i}: Actualizados {len(arrear_elements)} elementos")
                updated_loans.append({"loan_id": str(loan_id), "elements_updated": len(arrear_elements)})
            else:
                log(f"⚠️ Préstamo {i}: No se pudo actualizar")
        except Exception as e:
            log(f"❌ Error al actualizar préstamo {i}: {e}")

    amortization_updated.set(len(updated_loans))
    log(f"📊 Total préstamos actualizados: {len(updated_loans)}")
    return updated_loans

def validate_user_status(db, loan_documents):
    log(f"🔍 Validando status de {len(loan_documents)} usuarios...")
    user_collection = db.user
    loan_collection = db.loan
    validation_results = []
    updated_users = []

    unique_user_ids = set(loan_doc.get("user_id") for loan_doc in loan_documents if loan_doc.get("user_id"))
    log(f"📊 Procesando {len(unique_user_ids)} usuarios únicos...")

    for user_id in unique_user_ids:
        user_doc = user_collection.find_one({"_id": user_id})
        if not user_doc:
            log(f"❌ Usuario ID={user_id} no encontrado")
            validation_results.append({"user_id": str(user_id), "user_found": False})
            continue

        user_status = user_doc.get("status", "No especificado")
        if user_status == "arrear":
            user_loans = list(loan_collection.find({"user_id": user_id}))
            arrear_loans = [loan for loan in user_loans if loan.get("status") == "arrear"]
            should_update = len(user_loans) == 1 or len(arrear_loans) == 0
            if should_update:
                try:
                    result = user_collection.update_one({"_id": user_id}, {"$set": {"status": "active"}})
                    if result.modified_count > 0:
                        log(f"🔄 Usuario {user_id} actualizado a active")
                        updated_users.append({"user_id": str(user_id), "old_status": "arrear", "new_status": "active"})
                except Exception as e:
                    log(f"❌ Error actualizando usuario {user_id}: {e}")

        validation_results.append({"user_id": str(user_id), "user_status": user_status, "user_found": True})

    users_updated.set(len(updated_users))
    log(f"📊 Total usuarios actualizados: {len(updated_users)}")
    return validation_results, updated_users

def main():
    log("🚀 Iniciando script")

    # Arrancar servidor Prometheus en segundo plano
    start_metrics_server(8000)

    client = connect_to_mongodb(MONGODB_URI)
    if not client:
        return

    try:
        db = client[DATABASE_NAME]
        log(f"📂 Conectado a la base de datos: {DATABASE_NAME}")

        loan_documents = get_loan_documents(db)
        if not loan_documents:
            log("⚠️ No se encontraron documentos")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_json(loan_documents, f"{output_dir}/loan_documents_{timestamp}.json")

        amortization_updates = update_amortization_arrears(db, loan_documents)
        validation_results, updated_users = validate_user_status(db, loan_documents)

        save_to_json(validation_results, f"{output_dir}/user_validation_{timestamp}.json")
        if updated_users:
            save_to_json(updated_users, f"{output_dir}/user_updates_{timestamp}.json")
        if amortization_updates:
            save_to_json(amortization_updates, f"{output_dir}/amortization_updates_{timestamp}.json")

        log(f"📊 RESUMEN FINAL: loans={len(loan_documents)}, "
            f"amortization_updates={len(amortization_updates)}, "
            f"usuarios_validados={len(validation_results)}, usuarios_actualizados={len(updated_users)}")

    except Exception as e:
        log(f"❌ Error durante la ejecución: {e}")
    finally:
        client.close()
        log("🔌 Conexión cerrada")

if __name__ == "__main__":
    main()
