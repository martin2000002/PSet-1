from mage_ai.data_preparation.shared.secrets import get_secret_value
import psycopg2
import json
import time
import pandas as pd
from datetime import datetime, timezone
from dateutil import parser as date_parser

MAX_DB_RETRIES = 3
DB_RETRY_BACKOFF = 2

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def get_db_connection_with_retry(db_params, logger):
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
    logger = kwargs.get('logger')
    start_time_load = time.time()
    
    entity = 'Invoice'
    table_name = f"qb_{entity.lower()}"
    schema_name = "raw"
    
    if df is None or df.empty:
        logger.warning(f"[VOLUMETRY] No hay datos para la entidad {table_name}. Fin de ejecución.")
        return

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

    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        
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
        logger.info(f"[DDL] Tabla {schema_name}.{table_name} creada/verificada exitosamente.")
    except Exception as e:
        logger.error(f"[DDL] Error creando infraestructura RAW: {str(e)}")
        conn.rollback()
        raise e

    try:
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        count_before = cur.fetchone()[0]
    except:
        count_before = 0

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
    chunk_metrics = {}
    
    try:
        for _, row in df.iterrows():
            if not row['id'] or pd.isna(row['id']):
                logger.error(f"[VALIDATION] Registro con ID nulo omitido en exporter.")
                rows_skipped_null_id += 1
                continue
            
            chunk_key = f"{row['extract_window_start_utc']}|{row['extract_window_end_utc']}"
            if chunk_key not in chunk_metrics:
                chunk_metrics[chunk_key] = {
                    'count': 0,
                    'window_start': row['extract_window_start_utc'],
                    'window_end': row['extract_window_end_utc']
                }
            chunk_metrics[chunk_key]['count'] += 1
            
            ingested_at = row['ingested_at_utc']
            window_end_str = row['extract_window_end_utc']
            source_updated = row.get('source_last_updated_utc', '')
            
            try:
                window_end_dt = date_parser.parse(window_end_str) if isinstance(window_end_str, str) else window_end_str
                if ingested_at.tzinfo is None:
                    ingested_at = ingested_at.replace(tzinfo=timezone.utc)
                if window_end_dt.tzinfo is None:
                    window_end_dt = window_end_dt.replace(tzinfo=timezone.utc)
                    
                if ingested_at < window_end_dt:
                    rows_with_temporal_issues += 1
                    logger.debug(f"[TEMPORAL-WARNING] Registro {row['id']}: ingested_at < extract_window_end")
            except Exception as parse_error:
                logger.debug(f"[TEMPORAL-PARSE] No se pudo validar temporalidad para {row['id']}: {parse_error}")
            
            source_updated_ts = None
            if source_updated:
                try:
                    source_updated_ts = date_parser.parse(source_updated)
                except:
                    source_updated_ts = None
            
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

    # Metricas
    try:
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        count_after = cur.fetchone()[0]
        
        new_inserts = count_after - count_before
        updates = rows_processed - new_inserts if rows_processed > new_inserts else 0

        if new_inserts < 0:
            new_inserts = 0
            updates = rows_processed
        omitted = len(df) - rows_processed
        
        logger.info("--- REPORTE DE CALIDAD ---")
        logger.info(f"[QUALITY] Entidad: {table_name}")
        logger.info(f"[QUALITY] Registros en DataFrame: {len(df)}")
        logger.info(f"[QUALITY] Total registros en tabla (antes): {count_before}")
        logger.info(f"[QUALITY] Total registros en tabla (después): {count_after}")
        logger.info(f"[METRICS-GRANULAR] Nuevas inserciones: {new_inserts}")
        logger.info(f"[METRICS-GRANULAR] Actualizaciones (updates): {updates}")
        logger.info(f"[METRICS-GRANULAR] Omitidos/errores: {omitted}")
        logger.info(f"[IDEMPOTENCY] Upsert aplicado correctamente sobre PK 'id'.")
        
        if rows_with_temporal_issues > 0:
            logger.warning(f"[TEMPORAL-QUALITY] {rows_with_temporal_issues} registros con posibles "
                           f"inconsistencias temporales (ingested_at < extract_window_end).")
        
        if rows_skipped_null_id > 0:
            logger.warning(f"[INTEGRITY] {rows_skipped_null_id} registros omitidos por ID nulo.")
        
        logger.info("--- VOLUMETRÍA POR TRAMO ---")
        for chunk_key, metrics in chunk_metrics.items():
            logger.info(f"[CHUNK-VOLUMETRY] Ventana: [{metrics['window_start']} - {metrics['window_end']}] | "
                        f"Registros: {metrics['count']}")

            if metrics['count'] == 0:
                logger.warning(f"[VOLUMETRY] ALERTA: Tramo vacío detectado: {chunk_key}")
        logger.info(f"[VOLUMETRY] Total tramos procesados: {len(chunk_metrics)}")
        
        if rows_processed == 0 and not df.empty:
            logger.warning("[QUALITY] ALERTA: El DataFrame tenía datos pero no se procesó nada en Postgres.")

    except Exception as e:
        logger.warning(f"[QUALITY] No se pudo generar reporte de volumetría: {str(e)}")

    try:
        pipeline_failed = False
        if hasattr(df, 'attrs'):
            pipeline_failed = df.attrs.get('pipeline_failed', False)
        
        if pipeline_failed:
            logger.warning(f"[EXPORTER] Datos de extracción parcial exportados exitosamente.")
            logger.warning(f"[EXPORTER] Revisar logs del Loader para instrucciones de reanudación.")
        else:
            logger.info(f"[EXPORTER] Pipeline completado exitosamente.")
    except Exception as e:
        logger.warning(f"[EXPORTER] No se pudo verificar estado del pipeline: {str(e)}")
    
    finally:
        cur.close()
        conn.close()

    # Resumen final
    duration = round(time.time() - start_time_load, 2)
    logger.info("--- RESUMEN FINAL ---")
    logger.info(f"Registros procesados: {rows_processed}")
    logger.info(f"Nuevos: {new_inserts} | Actualizados: {updates} | Omitidos: {omitted}")
    logger.info(f"Duración: {duration} segundos")
    logger.info(f"Coherencia Temporal: Marcas registradas en UTC")
    logger.info("--------------------------------------------")