from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.db.models import Secret
from mage_ai.data_preparation.shared.secrets import get_secret_value
import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def test_hardcore_rotation(*args, **kwargs):
    # 1. Parámetros de prueba
    # ASEGÚRATE de haber creado este secreto en Settings -> Secrets antes
    target_secret = 'TEST_ROTATION_TOKEN' 
    nuevo_valor = f"TOKEN_GENERADO_{datetime.datetime.now().strftime('%H%M%S')}"
    
    print(f"--- INICIANDO PRUEBA DE ROTACIÓN ---")
    print(f"Buscando secreto: {target_secret}")
    
    try:
        # 2. Abrir sesión de base de datos interna de Mage
        with db_connection.session_scope() as session:
            print("Conexión a la DB de Mage abierta exitosamente.")
            
            # 3. Buscar el registro del secreto
            secret_record = session.query(Secret).filter(Secret.name == target_secret).first()
            
            if secret_record:
                print(f"Secreto encontrado. Valor actual: {secret_record.value}")
                print(f"Intentando cambiar a: {nuevo_valor}")
                
                # 4. Actualizar el valor
                secret_record.value = nuevo_valor
                session.add(secret_record)
                
                print("Cambio aplicado en la sesión. Esperando commit...")
            else:
                print(f"ERROR: El secreto '{target_secret}' no existe en Mage Secrets.")
                return {"status": "not_found"}

        # Al salir del bloque 'with', Mage hace el commit automático
        print("Commit realizado con éxito.")

        # 5. Verificación final usando la función estándar
        verificacion = get_secret_value(target_secret)
        print(f"VERIFICACIÓN FINAL: El nuevo valor recuperado es: {verificacion}")
        
        return {
            "status": "success",
            "valor_nuevo": verificacion,
            "coincide": verificacion == nuevo_valor
        }

    except Exception as e:
        print(f"FALLO CRÍTICO: {str(e)}")
        # Importante para que el bloque se ponga en ROJO si falla
        raise e