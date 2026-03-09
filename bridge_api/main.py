import os
import asyncio
import hmac
import hashlib
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# Configuración de registros
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BridgeAPI")

load_dotenv()

app = FastAPI(title="Marketing Bridge API v3.0 (Asíncrona)")

# --- CONFIGURACIÓN DE BASE DE DATOS (SQLAlchemy) ---
mysql_user = os.getenv('MYSQL_USER')
mysql_pass = os.getenv('MYSQL_PASS', '').strip('\"')
mysql_host = os.getenv('MYSQL_HOST')
mysql_port = os.getenv('MYSQL_PORT', 3306)
mysql_db = os.getenv('MYSQL_DB')

MYSQL_URL = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}:{mysql_port}/{mysql_db}"
engine = create_engine(MYSQL_URL, pool_size=10, max_overflow=20, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- CONFIGURACIÓN DE NEGOCIO ---
ACTIVE_EXP_DATE = 1924988400 
CONCURRENCY_LIMIT = 20 # Límite de peticiones simultáneas a Odoo

class OdooClient:
    def __init__(self):
        self.base_url = os.getenv('ODOO_URL', 'https://erp.boomsolutions.com').rstrip('/')
        self.endpoint = f"{self.base_url}/api/v1/partners/info"
        self.secret_key = os.getenv('ODOO_API_KEY', '')
        self.allowed_ip = os.getenv('ALLOWED_IP', '127.0.0.1')

    def generate_signature(self, params: Dict[str, str]) -> str:
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        query_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        return hmac.new(self.secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def get_status_async(self, client: httpx.AsyncClient, identification: str):
        # 1. Generar variaciones inteligentes de la cédula para 100% de precisión en el Match
        variations = [identification]
        
        # A. Si tiene guion final (ej. -5), agregamos variación sin él
        if '-' in identification:
            parts = identification.rsplit('-', 1)
            if parts[-1].isdigit() and len(parts[-1]) <= 2:
                variations.append(parts[0])
                
        # B. Generar variaciones limpias omitiendo letras como V-, J- (por si Odoo solo tiene los números)
        for base_id in list(variations):
            cleaned = base_id
            for prefix in ['V-', 'J-', 'E-', 'G-', 'V', 'J', 'E', 'G']:
                if cleaned.upper().startswith(prefix):
                    cleaned = cleaned[len(prefix):].lstrip('-')
                    break
            if cleaned and cleaned != base_id:
                variations.append(cleaned)
                
        # C. Generar versión sin guiones en general
        extra = [v.replace('-', '') for v in variations if '-' in v]
        variations.extend(extra)
        
        # Eliminar duplicados manteniendo el orden
        seen = set()
        unique_variations = [v for v in variations if v and not (v in seen or seen.add(v))]

        errors = []
        for search_id in unique_variations:
            try:
                params = {"identification_number": search_id}
                headers = {
                    "X-Api-Key": self.generate_signature(params),
                    "X-Forwarded-For": self.allowed_ip,
                    "Content-Type": "application/json"
                }
                
                res = await client.get(self.endpoint, params=params, headers=headers, timeout=12.0)
                
                if res.status_code == 200:
                    data = res.json()
                    partners = data.get("partners", [])
                    if partners:
                        partner = partners[0]
                        financials = partner.get("financials", {})
                        contract_status = financials.get("contract_status")
                        
                        if not contract_status and partner.get("contracts"):
                            contract_status = partner.get("contracts")[0].get("state_service")

                        state = (contract_status or "active").lower()
                        return {
                            "username": identification,
                            "active": state == 'active',
                            "iptv_username": search_id, # Guardamos el ID que sí hizo match
                            "state": state
                        }
                elif res.status_code == 404:
                    continue # No encontrado, probamos la siguiente variación
                else:
                    errors.append(f"HTTP {res.status_code}")
                    
            except httpx.TimeoutException:
                logger.warning(f"Timeout al consultar a Odoo por {search_id}")
                errors.append("TIMEOUT")
            except Exception as e:
                logger.error(f"Craso error de red consultando {search_id}: {e}")
                errors.append("NETWORK_ERROR")

        # Manejo de Errores Críticos: Si falló por Timeout o Servidor, abortamos
        if any(e in ("TIMEOUT", "NETWORK_ERROR") or "HTTP 5" in e for e in errors):
            logger.error(f"Falla crítica consultando {identification}. Se cancela acción.")
            return {"username": identification, "active": None, "state": None}
            
        # Si agotamos todas las variaciones y todas dieron 404, dictaminamos que NO EXISTE
        return {"username": identification, "active": False, "iptv_username": None, "state": "not_found"}

# --- MOTOR DE SINCRONIZACIÓN ASÍNCRONO ---

async def run_sync_logic():
    habilitado = os.getenv("SYNC_ENABLED", "false").lower() == "true"
    logger.info(f"== INICIO SYNC ASÍNCRO [MODO: {'PRODUCCIÓN' if habilitado else 'SIMULACIÓN'}] ==")
    
    odoo = OdooClient()
    
    with get_db() as db:
        # 0. Asegurar que exista la columna de alerta
        try:
            db.execute(text("ALTER TABLE users ADD COLUMN sync_alert VARCHAR(255) DEFAULT ''"))
            db.commit()
        except:
            db.rollback()

        # 1. Obtener usuarios de la BD (sin LIMIT 200 para procesar a todos los usuarios)
        result = db.execute(text("SELECT username, enabled, exp_date, admin_notes FROM users"))
        usuarios = result.mappings().all()
        
        if not usuarios:
            logger.info("No hay usuarios para procesar.")
            return

        usuarios_dict = {u['username']: u for u in usuarios}

        # 2. Consultar Odoo en paralelo con límite de concurrencia
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        async with httpx.AsyncClient() as client:
            async def limited_fetch(username):
                async with semaphore:
                    return await odoo.get_status_async(client, username)
            
            tasks = [limited_fetch(u['username']) for u in usuarios]
            resultados_odoo = await asyncio.gather(*tasks)

        # 3. Aplicar cambios en lote o secuencialmente (BD)
        for res in resultados_odoo:
            if res['state'] is None: continue
            
            db_user = usuarios_dict.get(res['username'])
            if not db_user: continue

            objetivo = res['iptv_username'] if res['iptv_username'] else res['username']
            nota = f"Odoo:{res['state']}"
            
            iptv_is_active = (db_user['enabled'] == 1)
            odoo_is_active = res['active']
            
            inconsistencia = False
            alerta = ""

            if iptv_is_active and not odoo_is_active:
                inconsistencia = True
                if res['state'] == 'not_found':
                    alerta = "ALERTA_FANTASMA (Activo en IPTV pero NO EXISTE en Odoo. NO CORTADO AUTOMÁTICAMENTE)"
                    logger.warning(f"[ALERTA GRAVE] {objetivo} - {alerta}")
                    # CRÍTICO: No lo cortamos automáticamente para evitar masacres de clientes viejos
                    if habilitado and nota != db_user.get('admin_notes'):
                        db.execute(
                            text("UPDATE users SET admin_notes = :nota, sync_alert = :alerta WHERE username = :user"),
                            {"nota": nota, "user": objetivo, "alerta": alerta}
                        )
                    continue # Saltamos la lógica de corte base
                else:
                    alerta = "CORTE_FORZADO (Desactivado por Odoo pero estaba activo en IPTV)"
                    logger.info(f"[INCONSISTENCIA BAJA] {objetivo} - {alerta}")
            elif not iptv_is_active and odoo_is_active:
                inconsistencia = True
                alerta = "ALTA_FORZADA (Activado por Odoo pero estaba cortado en IPTV)"
                logger.info(f"[INCONSISTENCIA ALTA] {objetivo} - {alerta}")

            # Solo actualizamos la base de datos si hay una inconsistencia o si la nota cambia (para mantener los registros frescos)
            # o si nunca hemos procesado a este usuario.
            
            if res['active']:
                if habilitado and (inconsistencia or nota != db_user.get('admin_notes')):
                    query_str = "UPDATE users SET enabled = 1, admin_enabled = 1, exp_date = :exp, admin_notes = :nota"
                    if inconsistencia: query_str += ", sync_alert = :alerta"
                    query_str += " WHERE username = :user"
                    
                    params = {"exp": ACTIVE_EXP_DATE, "nota": nota, "user": objetivo}
                    if inconsistencia: params["alerta"] = alerta

                    db.execute(text(query_str), params)
                    logger.info(f"[ALTA] {objetivo} (Odoo: {res['state']})")
            else:
                if habilitado and (inconsistencia or nota != db_user.get('admin_notes')):
                    query_str = "UPDATE users SET enabled = 0, admin_notes = :nota"
                    if inconsistencia: query_str += ", sync_alert = :alerta"
                    query_str += " WHERE username = :user"
                    
                    params = {"nota": nota, "user": objetivo}
                    if inconsistencia: params["alerta"] = alerta

                    db.execute(text(query_str), params)
                    logger.info(f"[BAJA] {objetivo} (Odoo: {res['state']})")
        
        if habilitado:
            db.commit()
            
    logger.info("== CICLO DE SINCRONIZACIÓN FINALIZADO ==")

def sync_wrapper():
    """Wrapper para ejecutar la corrutina en el scheduler de APScheduler."""
    asyncio.run(run_sync_logic())

# --- API Y TAREAS ---
scheduler = BackgroundScheduler()
scheduler.add_job(sync_wrapper, 'interval', minutes=10, id="job_sync")
scheduler.start()

@app.get("/health")
def health():
    return {"status": "online", "sync_enabled": os.getenv("SYNC_ENABLED") == "true"}

@app.post("/sync-now")
async def trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_sync_logic)
    return {"detail": "Sincronización manual en segundo plano."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
