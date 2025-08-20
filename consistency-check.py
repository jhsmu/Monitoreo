import os
import json
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuración de la URI de MongoDB (puedes cambiar esto por una variable de entorno o input)
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
    "principal",
    "total_amount",
    "principal_payment_amount",
    "interest_amount",
    "taxes",
    "days_in_arrear",
    "pending_payment",
    "arrear_interest_amount",
    "pending_principal_payment_amount",
    "pending_interest_amount",
    "pending_interest_taxes_amount",
    "pending_arrear_interest_amount",
    "pending_guarantee_amount",
    "pending_guarantee_taxes_amount",
    "pending_other_expenses_amount",
    "period_days",
    "interest_taxes_amount",
    "guarantee_amount",
    "guarantee_taxes_amount",
    "other_expenses_amount",
    "arrear_interest_paid",
    "arrear_interest_taxes_amount",
    "pending_arrear_interest_taxes_amount",
]

def connect_to_mongodb(uri):
    """Conecta a MongoDB Atlas usando la URI proporcionada"""
    try:
        client = MongoClient(uri)
        # Verificar la conexión
        client.admin.command('ping')
        print("✅ Conexión exitosa a MongoDB Atlas")
        return client
    except Exception as e:
        print(f"❌ Error al conectar a MongoDB: {e}")
        return None

def get_loan_documents(db):
    """Obtiene los documentos de la colección loan según los criterios especificados"""
    try:
        # Consulta equivalente a la del mongo shell
        query = {
            "financial_entity_id": {
                "$in": [
                    STOP_ID,
                    YOYO_ID
                ]
            },
            "status": "paid",
            "amortization": {
                "$elemMatch": {
                    "days_in_arrear": {"$gt": 0}
                }
            }
        }
        
        loan_collection = db.loan
        results = list(loan_collection.find(query))
        
        return results
        
    except Exception as e:
        print(f"❌ Error al consultar la colección loan: {e}")
        return []

def save_to_json(data, filename):
    """Guarda los datos en un archivo JSON"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return True

    except Exception as e:
        print(f"❌ Error al guardar el archivo JSON: {e}")
        return False

def update_amortization_arrears(db, loan_documents):
    """Actualiza los elementos de amortization que tengan days_in_arrear mayor a cero"""
    try:
        loan_collection = db.loan
        updated_loans = []
        
        print(f"\n🔄 Actualizando amortization para {len(loan_documents)} préstamos...")
        
        for i, loan_doc in enumerate(loan_documents, 1):
            loan_id = loan_doc.get("_id")
            print(f"🔍 Préstamo {i}: ID={loan_id}")
            
            amortization = loan_doc.get("amortization", [])
            
            if not amortization:
                print(f"⚠️  Préstamo {i}: No tiene amortization")
                continue
            
            # Contar elementos con days_in_arrear > 0
            arrear_elements = []
            update_array = []
            for j, element in enumerate(amortization):
                print("element", element)
                days_in_arrear = element.get("days_in_arrear", 0)
                days_in_arrear = int(days_in_arrear)
                print(f"🔍 Préstamo {i}: Elemento {j}: days_in_arrear={days_in_arrear}")
                print("type(days_in_arrear)", type(days_in_arrear))
                if days_in_arrear > 0:
                    arrear_elements.append({
                        "index": j,
                        "days_in_arrear": days_in_arrear
                    })
                    updated_element = element.copy()
                    updated_element["days_in_arrear"] = 0
                    update_array.append(updated_element)
                else:
                    update_array.append(element)

                type_check = all([isinstance(element[key], int) for key in int_keys])
                if not type_check:
                    print(f"Crédito con id {loan_id} tiene campos flotantes en la tabla de amortización {element.get('id')}")
                
            
            if not arrear_elements:
                print(f"ℹ️  Préstamo {i}: No tiene elementos con days_in_arrear > 0")
                continue
            
            print(f"📋 Préstamo {i}: Encontrados {len(arrear_elements)} elementos con days_in_arrear > 0")
            
            # Actualizar en MongoDB
            try:
                update_result = loan_collection.update_one(
                    {"_id": loan_id},
                    {"$set": {"amortization": update_array}}
                )
                
                if update_result.modified_count > 0:
                    print(f"✅ Préstamo {i}: Actualizados {len(arrear_elements)} elementos de amortization")
                    updated_loans.append({
                        "loan_id": str(loan_id),
                        "elements_updated": len(arrear_elements),
                        "arrear_elements": arrear_elements
                    })
                else:
                    print(f"⚠️  Préstamo {i}: No se pudo actualizar")
                    
            except Exception as update_error:
                print(f"❌ Error al actualizar préstamo {i}: {update_error}")
        
        # Resumen de actualizaciones
        if updated_loans:
            print(f"\n📊 RESUMEN DE ACTUALIZACIONES DE AMORTIZATION:")
            print(f"   • Préstamos actualizados: {len(updated_loans)}")
            total_elements = sum(loan["elements_updated"] for loan in updated_loans)
            print(f"   • Elementos de amortization actualizados: {total_elements}")
        else:
            print(f"\n📊 No se realizaron actualizaciones de amortization")
        
        return updated_loans
        
    except Exception as e:
        print(f"❌ Error al actualizar amortization: {e}")
        return []

def validate_user_status(db, loan_documents):
    """Valida el status de los usuarios asociados a los préstamos y actualiza según criterios"""
    try:
        user_collection = db.user
        loan_collection = db.loan
        validation_results = []
        updated_users = []
        
        print(f"\n🔍 Validando status de {len(loan_documents)} usuarios...")
        
        # Crear un set de user_ids únicos para evitar procesar el mismo usuario múltiples veces
        unique_user_ids = set()
        for loan_doc in loan_documents:
            user_id = loan_doc.get("user_id")
            if user_id:
                unique_user_ids.add(user_id)
        
        print(f"📊 Procesando {len(unique_user_ids)} usuarios únicos...")
        
        for user_id in unique_user_ids:
            # Buscar el usuario por _id
            user_query = {"_id": user_id}
            user_doc = user_collection.find_one(user_query)
            
            if not user_doc:
                print(f"❌ Usuario ID={user_id} - No encontrado en la colección user")
                validation_results.append({
                    "user_id": str(user_id),
                    "user_status": "No encontrado",
                    "user_found": False,
                    "loans_found": 0,
                    "status_updated": False
                })
                continue
            
            user_status = user_doc.get("status", "No especificado")
            print(f"\n👤 Procesando usuario: ID={user_id}, Status actual={user_status}")
            
            # Si el usuario tiene status "arrear", buscar todos sus préstamos
            if user_status == "arrear":
                print(f"🔍 Usuario en arrear - buscando todos sus préstamos...")
                
                # Buscar todos los préstamos del usuario
                user_loans_query = {"user_id": user_id}
                user_loans = list(loan_collection.find(user_loans_query))
                
                print(f"📋 Encontrados {len(user_loans)} préstamos para el usuario")
                
                # Contar préstamos con status "arrear"
                arrear_loans = [loan for loan in user_loans if loan.get("status") == "arrear"]
                other_loans = [loan for loan in user_loans if loan.get("status") != "arrear"]
                
                print(f"   • Préstamos en arrear: {len(arrear_loans)}")
                print(f"   • Otros préstamos: {len(other_loans)}")
                
                should_update = False
                update_reason = ""
                
                # Lógica de actualización
                if len(user_loans) == 1:
                    # Solo tiene un préstamo
                    should_update = True
                    update_reason = "Usuario tiene solo un préstamo"
                    print(f"✅ Usuario tiene solo un préstamo - marcado para actualización")
                elif len(arrear_loans) == 0:
                    # No tiene préstamos en arrear
                    should_update = True
                    update_reason = "Usuario no tiene préstamos en arrear"
                    print(f"✅ Usuario no tiene préstamos en arrear - marcado para actualización")
                else:
                    update_reason = "Usuario tiene múltiples préstamos y algunos están en arrear"
                    print(f"⚠️  Usuario tiene {len(arrear_loans)} préstamos en arrear - no se actualiza")
                
                # Actualizar status si corresponde
                if should_update:
                    try:
                        update_result = user_collection.update_one(
                            {"_id": user_id},
                            {"$set": {"status": "active"}}
                        )
                        
                        if update_result.modified_count > 0:
                            print(f"🔄 Status actualizado de 'arrear' a 'active'")
                            updated_users.append({
                                "user_id": str(user_id),
                                "old_status": "arrear",
                                "new_status": "active",
                                "reason": update_reason
                            })
                        else:
                            print(f"⚠️  No se pudo actualizar el status")
                            
                    except Exception as update_error:
                        print(f"❌ Error al actualizar status: {update_error}")
                
                validation_results.append({
                    "user_id": str(user_id),
                    "user_status": user_status,
                    "user_found": True,
                    "loans_found": len(user_loans),
                    "arrear_loans": len(arrear_loans),
                    "other_loans": len(other_loans),
                    "status_updated": should_update,
                    "update_reason": update_reason
                })
                
            else:
                # Usuario no está en arrear, solo registrar
                print(f"ℹ️  Usuario no está en arrear (status: {user_status})")
                validation_results.append({
                    "user_id": str(user_id),
                    "user_status": user_status,
                    "user_found": True,
                    "loans_found": 0,
                    "status_updated": False
                })
        
        # Resumen de actualizaciones
        if updated_users:
            print(f"\n📊 RESUMEN DE ACTUALIZACIONES:")
            print(f"   • Usuarios actualizados: {len(updated_users)}")
            for user in updated_users:
                print(f"   • {user['user_id']}: {user['old_status']} → {user['new_status']} ({user['reason']})")
        else:
            print(f"\n📊 No se realizaron actualizaciones de status")
        
        return validation_results, updated_users
        
    except Exception as e:
        print(f"❌ Error al validar usuarios: {e}")
        return [], []

def main():
    """Función principal del script"""
    print("🚀 Iniciando script de consulta MongoDB Atlas")
    print("=" * 50)
    
    # Solicitar la URI de MongoDB Atlas
    uri = MONGODB_URI
    
    # Conectar a MongoDB
    client = connect_to_mongodb(uri)
    if not client:
        return
    
    try:
        # Seleccionar la base de datos middleware
        db = client[DATABASE_NAME]
        print(f"📂 Conectado a la base de datos: middleware")
        
        # Paso 1: Obtener documentos de la colección loan
        print("\n📋 Paso 1: Consultando colección loan...")
        loan_documents = get_loan_documents(db)
        
        if not loan_documents:
            print("⚠️  No se encontraron documentos que cumplan los criterios")
            return
        
        # Paso 2: Guardar resultados en archivo JSON
        print("\n📋 Paso 2: Guardando resultados en archivo JSON...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/loan_documents_{timestamp}.json"
        
        if save_to_json(loan_documents, filename):
            print(f"📄 Archivo creado: {filename}")
        
        # Paso 3: Actualizar amortization
        print("\n📋 Paso 3: Actualizando amortization...")
        amortization_updates = update_amortization_arrears(db, loan_documents)

        # Paso 4: Validar status de usuarios
        print("\n📋 Paso 4: Validando status de usuarios...")
        validation_results, updated_users = validate_user_status(db, loan_documents)
        
        # Guardar resultados de validación
        validation_filename = f"{output_dir}/user_validation_{timestamp}.json"
        if save_to_json(validation_results, validation_filename):
            print(f"📄 Resultados de validación guardados en: {validation_filename}")
        
        # Guardar resultados de actualizaciones de usuarios
        if updated_users:
            user_updates_filename = f"{output_dir}/user_updates_{timestamp}.json"
            if save_to_json(updated_users, user_updates_filename):
                print(f"📄 Resultados de actualizaciones de usuarios guardados en: {user_updates_filename}")
        
        # Guardar resultados de actualizaciones de amortization
        if amortization_updates:
            amortization_updates_filename = f"amortization_updates_{timestamp}.json"
            if save_to_json(amortization_updates, amortization_updates_filename):
                print(f"📄 Resultados de actualizaciones de amortization guardados en: {amortization_updates_filename}")
        
        # Resumen final
        print("\n" + "=" * 50)
        print("📊 RESUMEN FINAL:")
        print(f"   • Documentos de loan encontrados: {len(loan_documents)}")
        print(f"   • Préstamos con amortization actualizada: {len(amortization_updates)}")
        print(f"   • Usuarios validados: {len(validation_results)}")
        print(f"   • Usuarios actualizados: {len(updated_users)}")
        
        files_generated = [filename, validation_filename]
        if updated_users:
            files_generated.append(user_updates_filename)
        if amortization_updates:
            files_generated.append(amortization_updates_filename)
        
        print(f"   • Archivos generados: {', '.join(files_generated)}")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
    
    finally:
        # Cerrar conexión
        client.close()
        print("🔌 Conexión cerrada")

if __name__ == "__main__":
    main()
