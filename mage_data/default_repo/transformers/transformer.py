import pandas as pd
from datetime import datetime
import time

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_raw_data(data, *args, **kwargs):
    """
    Añade metadatos obligatorios y realiza validaciones de integridad.
    Cumple con los requisitos 7.3 (Capa RAW), 7.4 (Calidad) y 7.5 (Métricas).
    """
    # 1. Obtener el logger y parámetros (Requisito 7.5)
    logger = kwargs.get('logger')
    start_time_transform = time.time()
    
    logger.info(f"--- INICIO DE FASE DE TRANSFORMACIÓN: {datetime.utcnow().isoformat()} UTC ---")
    
    start_date = kwargs.get('fecha_inicio')
    end_date = kwargs.get('fecha_fin')
    
    if not data:
        logger.warning("Fase de Transformación: No se recibieron datos del Loader. Omitiendo proceso.")
        return pd.DataFrame()

    transformed_rows = []
    skipped_records = 0
    duplicate_ids = set()
    seen_ids = set()

    # 2. Procesamiento y Validación (Requisito 7.4)
    logger.info(f"Procesando {len(data)} registros recibidos...")

    for item in data:
        record = item.get('raw_record')
        record_id = record.get('Id')

        # Validación de integridad: Clave primaria no nula (Requisito 7.4)
        if not record_id:
            logger.error("Validación de Integridad: Se encontró un registro sin ID. Omitiendo registro.")
            skipped_records += 1
            continue

        # Detección de duplicados en el lote actual (Requisito 7.4)
        if record_id in seen_ids:
            duplicate_ids.add(record_id)
            logger.warning(f"Validación de Integridad: ID {record_id} duplicado en el batch.")
        
        seen_ids.add(record_id)

        # 3. Construcción del objeto RAW con metadatos (Requisito 7.3)
        transformed_rows.append({
            'id': record_id,
            'payload': record, # JSON/JSONB completo
            'ingested_at_utc': datetime.utcnow(),
            'extract_window_start_utc': start_date,
            'extract_window_end_utc': end_date,
            'page_number': item.get('meta_page'),
            'page_size': item.get('meta_page_size'),
            'request_payload': item.get('meta_query')
        })

    # 4. Resumen de Métricas de Transformación (Requisito 7.5)
    duration = round(time.time() - start_time_transform, 4)
    
    logger.info("--- RESUMEN DE TRANSFORMACIÓN Y CALIDAD ---")
    logger.info(f"[METRIC] Registros procesados con éxito: {len(transformed_rows)}")
    logger.info(f"[METRIC] Registros omitidos por error: {skipped_records}")
    logger.info(f"[METRIC] IDs duplicados detectados: {len(duplicate_ids)}")
    logger.info(f"[METRIC] Duración de transformación: {duration} segundos")
    
    if skipped_records > 0:
        logger.error(f"Fase de Validación finalizada con {skipped_records} errores de integridad.")
    else:
        logger.info("Fase de Validación y Calidad finalizada exitosamente.")
        
    logger.info("--- FIN DE PROCESO TRANSFORMER ---")

    return pd.DataFrame(transformed_rows)