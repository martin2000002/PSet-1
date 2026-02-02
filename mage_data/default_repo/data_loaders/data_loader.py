from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests
import base64
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser

# --- CONFIGURACIÓN DE PARÁMETROS ---
CHUNK_DAYS = 1           # Tamaño del segmento (Requisito 7.1)
PAGE_SIZE = 20           # Registros por petición (Requisito 7.2)
MAX_RETRIES = 5          # Reintentos para Resiliencia
INITIAL_BACKOFF = 5      # Segundos base para Backoff
COURTESY_WAIT = 0.5      # Pausa entre páginas (aumentado para QBO)
CIRCUIT_BREAKER_THRESHOLD = 3  # Fallos consecutivos para activar circuit breaker

# URLs dinámicas según entorno (Requisito 6: sandbox/prod)
QBO_URLS = {
    'sandbox': "https://sandbox-quickbooks.api.intuit.com/v3/company",
    'production': "https://quickbooks.api.intuit.com/v3/company"
}
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


def get_new_access_token(client_id, client_secret, refresh_token, logger):
    """
    Fase Auth: Refresh Token al inicio de cada TRAMO (Requisito 7.2)
    IMPORTANTE: Captura y retorna el nuevo refresh_token para rotación.
    """
    logger.info(f"[AUTH] Iniciando autenticación OAuth 2.0...")
    
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    payload = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}
    
    response = requests.post(TOKEN_URL, headers=headers, data=payload)
    if response.status_code != 200:
        logger.error(f"[AUTH] ❌ Error Crítico OAuth: {response.text}")
        raise Exception(f"OAuth Failure: {response.status_code}")
    
    token_data = response.json()
    access_token = token_data.get('access_token')
    new_refresh_token = token_data.get('refresh_token')
    
    logger.info(f"[AUTH] ✅ Access Token obtenido exitosamente")
    
    # Rotación de Refresh Token: Log para actualización manual (SIN EXPONER EL TOKEN)
    if new_refresh_token and new_refresh_token != refresh_token:
        logger.warning(f"[AUTH-ROTATION] ⚠️ NUEVO REFRESH TOKEN EMITIDO.")
        logger.warning(f"[AUTH-ROTATION] Actualizar secreto QBO_REFRESH_TOKEN en Mage Secrets.")
        # SEGURIDAD: No loguear el token, solo notificar que cambió
        logger.info(f"[AUTH-ROTATION] Token rotado. Longitud: {len(new_refresh_token)} caracteres.")
    else:
        logger.info(f"[AUTH] Refresh Token sin cambios.")
    
    return access_token, new_refresh_token

@data_loader
def load_data_from_quickbooks(*args, **kwargs):
    """
    LOADER: Extrae datos de QuickBooks Online API.
    - Maneja chunks temporales
    - Paginación completa
    - Circuit breaker global
    - Métricas por tramo
    """
    logger = kwargs.get('logger')
    
    # ========== 1. PARÁMETROS Y VALIDACIONES (Requisito 3 y 7.1) ==========
    entity = kwargs.get('entity', 'Invoice')  # Invoice, Customer, Item
    start_date_str = kwargs.get('fecha_inicio')
    end_date_str = kwargs.get('fecha_fin')
    resume_from_str = kwargs.get('resume_from')  # Para reanudación (Requisito 7.5 Runbook)
    
    logger.info(f"[CONFIG] Entidad a extraer: {entity}")
    
    if not start_date_str or not end_date_str:
        raise ValueError("[VALIDATION] Error: 'fecha_inicio' y 'fecha_fin' son obligatorios.")

    # Parseo correcto de fechas con conversión a UTC (FIX: usar astimezone, no replace)
    # Esto maneja correctamente offsets como -08:00 (PST) convirtiéndolos a UTC
    def parse_to_utc(date_str):
        """Parsea fecha ISO y la convierte correctamente a UTC."""
        # Usar dateutil para manejar cualquier formato ISO con offset
        dt = date_parser.parse(date_str)
        # Si no tiene timezone, asumir UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # Convertir a UTC (astimezone convierte, replace solo cambia la etiqueta)
        return dt.astimezone(timezone.utc)
    
    dt_start = parse_to_utc(start_date_str)
    dt_end = parse_to_utc(end_date_str)
        
    # Validación: fecha_inicio < fecha_fin (prevenir bucle infinito)
    if dt_start >= dt_end:
        raise ValueError(f"[VALIDATION] Error: 'fecha_inicio' ({dt_start}) debe ser anterior a 'fecha_fin' ({dt_end}).")
    
    # Reanudación desde checkpoint (Requisito 7.5 Runbook)
    if resume_from_str:
        dt_resume = parse_to_utc(resume_from_str)
        if dt_resume > dt_start and dt_resume < dt_end:
            logger.info(f"[RESUME] Reanudando desde checkpoint: {resume_from_str}")
            dt_start = dt_resume
    
    # ========== 2. SECRETOS Y CONFIGURACIÓN (Requisito 6) ==========
    client_id = get_secret_value('QBO_CLIENT_ID')
    client_secret = get_secret_value('QBO_CLIENT_SECRET')
    refresh_token = get_secret_value('QBO_REFRESH_TOKEN')
    realm_id = get_secret_value('QBO_REALM_ID')
    qbo_environment = get_secret_value('QBO_ENVIRONMENT') or 'sandbox'  # sandbox/production
    
    # URL dinámica según entorno
    qbo_base_url = QBO_URLS.get(qbo_environment.lower(), QBO_URLS['sandbox'])
    logger.info(f"[CONFIG] Entorno QBO: {qbo_environment} | URL Base: {qbo_base_url}")
    
    # ========== 3. VARIABLES DE CONTROL ==========
    all_final_records = []
    current_date = dt_start
    current_refresh_token = refresh_token
    consecutive_failures = 0  # Para Circuit Breaker
    total_start_time = time.time()
    
    # Metadatos para el exporter (checkpoint info)
    last_successful_chunk_end = None
    chunk_index = 0  # Contador de tramos para simulación de fallos
    pipeline_failed = False  # Flag para indicar si hubo error
    original_fecha_fin = end_date_str  # Guardar fecha_fin original para comparar

    # ========== 4. BUCLE EXTERNO: SEGMENTACIÓN POR TRAMO (Requisito 7.1) ==========
    # Agregar 1 segundo al final para incluir el último registro exacto (>= y <=)
    dt_end_inclusive = dt_end + timedelta(seconds=1)
    
    while current_date < dt_end_inclusive:
        chunk_index += 1
        start_time_chunk = time.time()
        next_date = min(current_date + timedelta(days=CHUNK_DAYS), dt_end_inclusive)
        
        chunk_start = current_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        chunk_end = next_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
        try:
            # OAUTH 2.0: Refresh al inicio de cada TRAMO (Requisito 7.2)
            access_token, new_refresh_token = get_new_access_token(
                client_id, client_secret, current_refresh_token, logger
            )
            # Actualizar token para siguiente tramo si fue rotado
            if new_refresh_token:
                current_refresh_token = new_refresh_token
            
            logger.info(f"[CHUNK] --- Iniciando Tramo: {chunk_start} a {chunk_end} ---")
            
            start_position = 1
            more_data_in_chunk = True
            pages_in_chunk = 0
            records_in_chunk = 0
            
            # ========== 5. BUCLE MEDIO: PAGINACIÓN (Requisito 7.2) ==========
            while more_data_in_chunk:
                # Filtros históricos en UTC (Requisito 7.2)
                query = (f"SELECT * FROM {entity} "
                         f"WHERE Metadata.LastUpdatedTime >= '{chunk_start}' "
                         f"AND Metadata.LastUpdatedTime < '{chunk_end}' "
                         f"STARTPOSITION {start_position} MAXRESULTS {PAGE_SIZE}")
                
                url = f"{qbo_base_url}/{realm_id}/query"
                headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
                
                # ========== 6. BUCLE INTERNO: RESILIENCIA CON BACKOFF (Requisito 7.2) ==========
                retries = 0
                success = False
                response = None
                
                while retries < MAX_RETRIES and not success:
                    try:
                        response = requests.get(url, headers=headers, params={'query': query})
                        
                        if response.status_code == 200:
                            success = True
                            consecutive_failures = 0  # Reset circuit breaker
                        elif response.status_code == 429:
                            wait = (2 ** retries) * INITIAL_BACKOFF
                            logger.warning(f"[RATE-LIMIT] HTTP 429. Reintento {retries+1}/{MAX_RETRIES} en {wait}s")
                            time.sleep(wait)
                            retries += 1
                        elif response.status_code == 401:
                            # Token expirado, refrescar (NO cuenta como reintento)
                            logger.warning("[AUTH] Token expirado, refrescando...")
                            access_token, new_refresh_token = get_new_access_token(
                                client_id, client_secret, current_refresh_token, logger
                            )
                            if new_refresh_token:
                                current_refresh_token = new_refresh_token
                            headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
                            # NO incrementar retries: refresh de token no es un error real
                        else:
                            logger.error(f"[API-ERROR] HTTP {response.status_code}: {response.text}")
                            retries += 1
                            time.sleep(INITIAL_BACKOFF)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"[NETWORK-ERROR] {str(e)}. Reintento {retries+1}/{MAX_RETRIES}")
                        retries += 1
                        time.sleep((2 ** retries) * INITIAL_BACKOFF)

                if not success:
                    consecutive_failures += 1
                    logger.error(f"[CHUNK-FAIL] Tramo {chunk_start} falló después de {MAX_RETRIES} reintentos.")
                    
                    # ========== CIRCUIT BREAKER (Requisito 7.2) ==========
                    if consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
                        logger.critical(f"[CIRCUIT-BREAKER] ⛔ {consecutive_failures} fallos consecutivos. "
                                        f"Pipeline detenido. Último tramo exitoso: {last_successful_chunk_end}")
                        raise Exception(f"Circuit Breaker activado tras {consecutive_failures} fallos consecutivos.")
                    break  # Salir del bucle de paginación para este tramo

                # ========== 7. PROCESAMIENTO DE DATOS Y METADATOS (Requisito 7.3) ==========
                data_payload = response.json().get('QueryResponse', {}).get(entity, [])
                
                for record in data_payload:
                    # ========== VALIDACIÓN DE INTEGRIDAD: PK NO NULA (Requisito 7.4) ==========
                    record_id = record.get('Id')
                    if not record_id:
                        logger.error(f"[VALIDATION] ❌ Registro sin ID omitido. Payload parcial: {str(record)[:200]}")
                        continue  # Omitir registros sin PK
                    
                    # Validación de coherencia temporal del dato (Requisito 7.4)
                    record_last_updated = record.get('MetaData', {}).get('LastUpdatedTime', '')
                    
                    all_final_records.append({
                        'id': record.get('Id'),
                        'payload': record,
                        'ingested_at_utc': datetime.now(timezone.utc),
                        'extract_window_start_utc': chunk_start,
                        'extract_window_end_utc': chunk_end,
                        'page_number': (start_position // PAGE_SIZE) + 1,
                        'page_size': PAGE_SIZE,
                        'request_payload': query,
                        'source_last_updated_utc': record_last_updated  # Para validación
                    })
                
                pages_in_chunk += 1
                records_in_chunk += len(data_payload)
                
                # Condición de salida de paginación
                if len(data_payload) < PAGE_SIZE:
                    more_data_in_chunk = False
                else:
                    start_position += PAGE_SIZE
                    time.sleep(COURTESY_WAIT)

            # ========== 8. MÉTRICAS Y VOLUMETRÍA POR TRAMO (Requisito 7.1 y 7.5) ==========
            duration_chunk = round(time.time() - start_time_chunk, 2)
            
            # Detección de anomalías: días vacíos (Requisito 7.4)
            if records_in_chunk == 0:
                logger.warning(f"[VOLUMETRY] ⚠️ ALERTA: Tramo {chunk_start} a {chunk_end} retornó 0 registros. "
                               f"Verificar si es esperado o hay problema de filtros/datos.")
            
            logger.info(f"[METRICS] Tramo Finalizado: "
                        f"Ventana: [{chunk_start} - {chunk_end}] | "
                        f"Páginas: {pages_in_chunk} | "
                        f"Registros: {records_in_chunk} | "
                        f"Duración: {duration_chunk}s")
            
            # Actualizar checkpoint para reanudación
            last_successful_chunk_end = chunk_end
            
        except Exception as e:
            consecutive_failures += 1
            pipeline_failed = True
            logger.error(f"[CHUNK-ERROR] ❌ Error en tramo #{chunk_index} ({chunk_start}): {str(e)}")
            
            # Mostrar instrucciones de checkpoint para reanudación
            if last_successful_chunk_end:
                logger.critical(f"[CHECKPOINT] ═══════════════════════════════════════════════════════")
                logger.critical(f"[CHECKPOINT] ⛔ PIPELINE INTERRUMPIDO EN TRAMO #{chunk_index}")
                logger.critical(f"[CHECKPOINT] Último tramo exitoso: #{chunk_index - 1}")
                logger.critical(f"[CHECKPOINT] Último checkpoint: {last_successful_chunk_end}")
                logger.critical(f"[CHECKPOINT] ───────────────────────────────────────────────────────")
                logger.critical(f"[CHECKPOINT] 📋 PARA REANUDAR, usar parámetro:")
                logger.critical(f"[CHECKPOINT]    resume_from = '{last_successful_chunk_end}'")
                logger.critical(f"[CHECKPOINT] ═══════════════════════════════════════════════════════")
            else:
                logger.critical(f"[CHECKPOINT] ⛔ PIPELINE FALLÓ EN EL PRIMER TRAMO. No hay checkpoint disponible.")
            
            # Salir del bucle pero retornar los datos ya extraídos (tramos exitosos)
            logger.warning(f"[RECOVERY] Retornando {len(all_final_records)} registros de tramos exitosos anteriores.")
            break

        current_date = next_date

    # ========== 9. RESUMEN FINAL Y METADATOS PARA EXPORTER ==========
    total_duration = round(time.time() - total_start_time, 2)
    
    if pipeline_failed:
        logger.warning(f"[EXTRACTION-PARTIAL] === EXTRACCIÓN PARCIAL (CON ERRORES) ===")
        logger.warning(f"[EXTRACTION-PARTIAL] Tramos completados exitosamente: {chunk_index - 1}")
    else:
        logger.info(f"[EXTRACTION-COMPLETE] === EXTRACCIÓN FINALIZADA EXITOSAMENTE ===")
    
    logger.info(f"[EXTRACTION-COMPLETE] Total registros: {len(all_final_records)}")
    logger.info(f"[EXTRACTION-COMPLETE] Duración total: {total_duration}s")
    logger.info(f"[EXTRACTION-COMPLETE] Entidad: {entity}")
    logger.info(f"[EXTRACTION-COMPLETE] Rango solicitado: {start_date_str} a {end_date_str}")
    if resume_from_str:
        logger.info(f"[EXTRACTION-COMPLETE] Reanudado desde checkpoint: {resume_from_str}")
    
    if len(all_final_records) == 0:
        logger.warning("[VOLUMETRY] ⚠️ No se extrajeron registros. Verificar rango de fechas y datos en QBO.")
    
    # Crear DataFrame con metadatos de checkpoint para el exporter
    df = pd.DataFrame(all_final_records)
    
    # Agregar metadatos globales como atributos del DataFrame (para el exporter)
    if not df.empty:
        df.attrs['last_checkpoint'] = last_successful_chunk_end
        df.attrs['pipeline_failed'] = pipeline_failed
        df.attrs['original_fecha_fin'] = original_fecha_fin
    
    return df