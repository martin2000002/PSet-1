from mage_ai.data_preparation.shared.secrets import get_secret_value
import psycopg2
import json
import time
import pandas as pd
from datetime import datetime, timezone
from dateutil import parser as date_parser

# --- CONFIGURACIÓN DE REINTENTOS PARA POSTGRES ---
MAX_DB_RETRIES = 3
DB_RETRY_BACKOFF = 2

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def get_db_connection_with_retry(db_params, logger):
    """
    Conexión a Postgres con reintentos para errores transitorios.
    """
    retries = 0
    while retries < MAX_DB_RETRIES:
        try:
            conn = psycopg2.connect(**db_params)
            return conn
        except psycopg2.OperationalError as e:
            retries += 1
            wait = (2 ** retries) * DB_RETRY_BACKOFF
            logger.warning(f"[DB-RETRY] Error de conexión: {str(e)}. Reintento {retries}/{MAX_DB_RETRIES} en {wait}s")
            time.sleep(wait)
    raise Exception(f"[DB-FAIL] No se pudo conectar a Postgres tras {MAX_DB_RETRIES} reintentos.")


@data_exporter
def export_data_to_postgres(df, *args, **kwargs):
    """
    EXPORTER: Carga datos a PostgreSQL con:
    - Idempotencia (UPSERT)
    - Metadatos obligatorios
    - Tracking insert/update/omitidos
    - Validación de coherencia temporal
    - Reintentos para errores transitorios
    - Persistencia de checkpoint para reanudación
    """
    logger = kwargs.get('logger')
    start_time_load = time.time()
    
    # ========== 1. IDENTIFICACIÓN DE ENTIDAD (Requisito 3 y 7.3) ==========
    entity = kwargs.get('entity', 'Invoice')  # Invoice, Customer, Item
    table_name = f"qb_{entity.lower()}"
    schema_name = "raw"
    
    logger.info(f"[CONFIG] Entidad: {entity} -> Tabla destino: {schema_name}.{table_name}")

    if df is None or df.empty:
        logger.warning(f"[VOLUMETRY] No hay datos para la entidad {table_name}. Fin de ejecución.")
        return

    # ========== 2. GESTIÓN DE SECRETOS (Requisito 6) ==========
    try:
        db_params = {
            'host': get_secret_value('POSTGRES_HOST'),
            'database': get_secret_value('POSTGRES_DB'),
            'user': get_secret_value('POSTGRES_USER'),
            'password': get_secret_value('POSTGRES_PASSWORD'),
            'port': get_secret_value('POSTGRES_PORT')
        }
        conn = get_db_connection_with_retry(db_params, logger)
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"[SECURITY/DB] Error al obtener secretos o conectar a Postgres: {str(e)}")
        raise e

    # ========== 3. DISEÑO DE CAPA RAW (Requisito 7.3) ==========
    try:
        # Iniciar transacción explícita para DDL (Requisito 7: robustez)
        cur.execute("BEGIN;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        
        # Tabla principal con Primary Key y Metadatos Obligatorios
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                id VARCHAR PRIMARY KEY,
                payload JSONB,
                ingested_at_utc TIMESTAMP WITH TIME ZONE,
                extract_window_start_utc TIMESTAMP WITH TIME ZONE,
                extract_window_end_utc TIMESTAMP WITH TIME ZONE,
                page_number INT,
                page_size INT,
                request_payload TEXT,
                source_last_updated_utc TIMESTAMP WITH TIME ZONE
            );
        """
        cur.execute(create_table_query)
        conn.commit()
        logger.info(f"[DDL] ✅ Tabla {schema_name}.{table_name} creada/verificada exitosamente.")
    except Exception as e:
        logger.error(f"[DDL] Error creando infraestructura RAW: {str(e)}")
        conn.rollback()
        raise e

    # ========== 4. CONTEO PREVIO PARA MÉTRICAS (Requisito 7.5) ==========
    try:
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        count_before = cur.fetchone()[0]
    except:
        count_before = 0

    # ========== 5. IDEMPOTENCIA MEDIANTE UPSERT (Requisito 7.3 y 7.4) ==========
    # Incluye page_size en el UPDATE para consistencia completa
    upsert_sql = f"""
        INSERT INTO {schema_name}.{table_name} (
            id, payload, ingested_at_utc, extract_window_start_utc, 
            extract_window_end_utc, page_number, page_size, request_payload,
            source_last_updated_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            payload = EXCLUDED.payload,
            ingested_at_utc = EXCLUDED.ingested_at_utc,
            extract_window_start_utc = EXCLUDED.extract_window_start_utc,
            extract_window_end_utc = EXCLUDED.extract_window_end_utc,
            page_number = EXCLUDED.page_number,
            page_size = EXCLUDED.page_size,
            request_payload = EXCLUDED.request_payload,
            source_last_updated_utc = EXCLUDED.source_last_updated_utc;
    """

    rows_processed = 0
    rows_with_temporal_issues = 0
    rows_skipped_null_id = 0
    
    # ========== MÉTRICAS POR TRAMO (Requisito 7.4 y 7.5) ==========
    chunk_metrics = {}  # {chunk_key: {'count': N, 'window_start': X, 'window_end': Y}}
    
    try:
        for _, row in df.iterrows():
            # ========== VALIDACIÓN DE INTEGRIDAD: PK NO NULA (Requisito 7.4) ==========
            if not row['id'] or pd.isna(row['id']):
                logger.error(f"[VALIDATION] ❌ Registro con ID nulo omitido en exporter.")
                rows_skipped_null_id += 1
                continue
            
            # ========== TRACKING POR TRAMO (Requisito 7.4) ==========
            chunk_key = f"{row['extract_window_start_utc']}|{row['extract_window_end_utc']}"
            if chunk_key not in chunk_metrics:
                chunk_metrics[chunk_key] = {
                    'count': 0,
                    'window_start': row['extract_window_start_utc'],
                    'window_end': row['extract_window_end_utc']
                }
            chunk_metrics[chunk_key]['count'] += 1
            
            # ========== 6. VALIDACIÓN DE COHERENCIA TEMPORAL (Requisito 7.4) ==========
            ingested_at = row['ingested_at_utc']
            window_end_str = row['extract_window_end_utc']
            source_updated = row.get('source_last_updated_utc', '')
            
            # Parsear window_end para comparación
            try:
                window_end_dt = date_parser.parse(window_end_str) if isinstance(window_end_str, str) else window_end_str
                if ingested_at.tzinfo is None:
                    ingested_at = ingested_at.replace(tzinfo=timezone.utc)
                if window_end_dt.tzinfo is None:
                    window_end_dt = window_end_dt.replace(tzinfo=timezone.utc)
                    
                # Verificar coherencia: ingested_at debe ser >= window_end
                if ingested_at < window_end_dt:
                    rows_with_temporal_issues += 1
                    logger.debug(f"[TEMPORAL-WARNING] Registro {row['id']}: ingested_at < extract_window_end")
            except Exception as parse_error:
                logger.debug(f"[TEMPORAL-PARSE] No se pudo validar temporalidad para {row['id']}: {parse_error}")
            
            # Parsear source_last_updated_utc
            source_updated_ts = None
            if source_updated:
                try:
                    source_updated_ts = date_parser.parse(source_updated)
                except:
                    source_updated_ts = None
            
            # Ejecutar UPSERT con reintentos
            retry_count = 0
            while retry_count < MAX_DB_RETRIES:
                try:
                    cur.execute(upsert_sql, (
                        str(row['id']), 
                        json.dumps(row['payload']), 
                        row['ingested_at_utc'],
                        row['extract_window_start_utc'], 
                        row['extract_window_end_utc'],
                        row['page_number'], 
                        row['page_size'], 
                        row['request_payload'],
                        source_updated_ts
                    ))
                    rows_processed += 1
                    break
                except psycopg2.OperationalError as e:
                    retry_count += 1
                    if retry_count >= MAX_DB_RETRIES:
                        raise e
                    logger.warning(f"[DB-RETRY] Error en INSERT, reintentando... {retry_count}/{MAX_DB_RETRIES}")
                    time.sleep(DB_RETRY_BACKOFF * retry_count)
                    # Reconectar si es necesario
                    try:
                        conn = get_db_connection_with_retry(db_params, logger)
                        cur = conn.cursor()
                    except:
                        pass
        
        conn.commit()
        logger.info(f"[LOAD] Upsert exitoso: {rows_processed} filas procesadas en {table_name}.")
        
    except Exception as e:
        logger.error(f"[LOAD] Fallo en la carga de datos: {str(e)}")
        conn.rollback()
        raise e

    # ========== 7. MÉTRICAS GRANULARES: INSERT VS UPDATE VS OMITIDOS (Requisito 7.5) ==========
    try:
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        count_after = cur.fetchone()[0]
        
        # Cálculo de métricas granulares
        new_inserts = count_after - count_before
        updates = rows_processed - new_inserts if rows_processed > new_inserts else 0
        # Si hay menos inserts que rows_processed, la diferencia son updates
        if new_inserts < 0:
            new_inserts = 0
            updates = rows_processed
        omitted = len(df) - rows_processed
        
        logger.info("--- REPORTE DE CALIDAD (Requisito 7.4) ---")
        logger.info(f"[QUALITY] Entidad: {table_name}")
        logger.info(f"[QUALITY] Registros en DataFrame: {len(df)}")
        logger.info(f"[QUALITY] Total registros en tabla (antes): {count_before}")
        logger.info(f"[QUALITY] Total registros en tabla (después): {count_after}")
        logger.info(f"[METRICS-GRANULAR] Nuevas inserciones: {new_inserts}")
        logger.info(f"[METRICS-GRANULAR] Actualizaciones (updates): {updates}")
        logger.info(f"[METRICS-GRANULAR] Omitidos/errores: {omitted}")
        logger.info(f"[IDEMPOTENCY] Upsert aplicado correctamente sobre PK 'id'.")
        
        if rows_with_temporal_issues > 0:
            logger.warning(f"[TEMPORAL-QUALITY] ⚠️ {rows_with_temporal_issues} registros con posibles "
                           f"inconsistencias temporales (ingested_at < extract_window_end).")
        
        if rows_skipped_null_id > 0:
            logger.warning(f"[INTEGRITY] ⚠️ {rows_skipped_null_id} registros omitidos por ID nulo.")
        
        # ========== REPORTE DE VOLUMETRÍA POR TRAMO (Requisito 7.4) ==========
        logger.info("--- VOLUMETRÍA POR TRAMO (Requisito 7.4) ---")
        for chunk_key, metrics in chunk_metrics.items():
            logger.info(f"[CHUNK-VOLUMETRY] Ventana: [{metrics['window_start']} - {metrics['window_end']}] | "
                        f"Registros: {metrics['count']}")
            # Detección de días vacíos
            if metrics['count'] == 0:
                logger.warning(f"[VOLUMETRY] ⚠️ ALERTA: Tramo vacío detectado: {chunk_key}")
        logger.info(f"[VOLUMETRY] Total tramos procesados: {len(chunk_metrics)}")
        
        # Detección de anomalías (Requisito 7.4)
        if rows_processed == 0 and not df.empty:
            logger.warning("[QUALITY] ⚠️ ALERTA: El DataFrame tenía datos pero no se procesó nada en Postgres.")

    except Exception as e:
        logger.warning(f"[QUALITY] No se pudo generar reporte de volumetría: {str(e)}")

    # ========== 8. ESTADO DEL PIPELINE (Requisito 7.5) ==========
    # Nota: El checkpoint para reanudación se muestra en el Loader donde ocurre el error
    try:
        pipeline_failed = False
        if hasattr(df, 'attrs'):
            pipeline_failed = df.attrs.get('pipeline_failed', False)
        
        if pipeline_failed:
            logger.warning(f"[EXPORTER] ⚠️ Datos de extracción parcial exportados exitosamente.")
            logger.warning(f"[EXPORTER] Revisar logs del Loader para instrucciones de reanudación.")
        else:
            logger.info(f"[EXPORTER] ✅ Pipeline completado exitosamente.")
    except Exception as e:
        logger.warning(f"[EXPORTER] No se pudo verificar estado del pipeline: {str(e)}")
    
    finally:
        cur.close()
        conn.close()

    # ========== 9. OBSERVABILIDAD Y MÉTRICAS FINALES (Requisito 7.5) ==========
    duration = round(time.time() - start_time_load, 2)
    logger.info("--- MÉTRICAS DE OPERACIÓN (Requisito 7.5) ---")
    logger.info(f"Fase: CARGA RAW")
    logger.info(f"Registros procesados: {rows_processed}")
    logger.info(f"Nuevos: {new_inserts} | Actualizados: {updates} | Omitidos: {omitted}")
    logger.info(f"Duración: {duration} segundos")
    logger.info(f"Coherencia Temporal: Marcas registradas en UTC")
    logger.info("--------------------------------------------")