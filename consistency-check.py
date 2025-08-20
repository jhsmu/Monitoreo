import os
import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

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

def log(msg):
    """Función simple para imprimir con timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

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
        return results
    except Exception as e:
        log(f"❌ Error al consultar la colección loan: {e}")
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

# Resto de funciones (update_amortization_arrears, validate_user_status)
# Se agregan logs de la misma forma: log("mensaje...")

def main():
    log("🚀 Iniciando script de consulta MongoDB Atlas")
    client = connect_to_mongodb(MONGODB_URI)
    if not client:
        return

    try:
        db = client[DATABASE_NAME]
        log(f"📂 Conectado a la base de datos: {DATABASE_NAME}")

        # Paso 1
        loan_documents = get_loan_documents(db)
        if not loan_documents:
            log("⚠️ No se encontraron documentos que cumplan los criterios")
            return

        # Paso 2: Guardar JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/loan_documents_{timestamp}.json"
        save_to_json(loan_documents, filename)

        # Paso 3: Actualizar amortization
        log("📋 Actualizando amortization...")
        amortization_updates = update_amortization_arrears(db, loan_documents)

        # Paso 4: Validar status de usuarios
        log("📋 Validando status de usuarios...")
        validation_results, updated_users = validate_user_status(db, loan_documents)

        # Guardar resultados
        validation_filename = f"{output_dir}/user_validation_{timestamp}.json"
        save_to_json(validation_results, validation_filename)

        if updated_users:
            user_updates_filename = f"{output_dir}/user_updates_{timestamp}.json"
            save_to_json(updated_users, user_updates_filename)

        if amortization_updates:
            amortization_updates_filename = f"{output_dir}/amortization_updates_{timestamp}.json"
            save_to_json(amortization_updates, amortization_updates_filename)

        log(f"📊 RESUMEN FINAL: documentos={len(loan_documents)}, "
            f"amortization_actualizada={len(amortization_updates)}, "
            f"usuarios_validados={len(validation_results)}, usuarios_actualizados={len(updated_users)}")

    except Exception as e:
        log(f"❌ Error durante la ejecución: {e}")
    finally:
        client.close()
        log("🔌 Conexión cerrada")

if __name__ == "__main__":
    main()
