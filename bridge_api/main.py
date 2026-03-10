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

from urllib.parse import quote_plus

# --- CONFIGURACIÓN DE BASE DE DATOS (SQLAlchemy) ---
mysql_user = os.getenv('MYSQL_USER')
mysql_pass = os.getenv('MYSQL_PASS', '').strip('\"')
mysql_host = os.getenv('MYSQL_HOST')
mysql_port = os.getenv('MYSQL_PORT', 3306)
mysql_db = os.getenv('MYSQL_DB')

# Escapar la contraseña para evitar errores con caracteres especiales (como '#')
mysql_pass_escaped = quote_plus(mysql_pass)

MYSQL_URL = f"mysql+pymysql://{mysql_user}:{mysql_pass_escaped}@{mysql_host}:{mysql_port}/{mysql_db}"
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
    
    # Obtener lista de usuarios (Fuera del bloque con conexión larga)
    usuarios = []
    with get_db() as db:
        result = db.execute(text("SELECT username, enabled, exp_date, admin_notes FROM users ORDER BY id DESC"))
        usuarios = result.mappings().all()
    
    if not usuarios:
        logger.info("No hay usuarios para procesar.")
        return

    logger.info(f"== INICIO SYNC: {len(usuarios)} usuarios en lotes de {50} ==")

    BATCH_SIZE = 50
    CONCURRENCY_LOTE = 5 # Más conservador para Odoo

    for i in range(0, len(usuarios), BATCH_SIZE):
        lote = usuarios[i:i + BATCH_SIZE]
        
        # 1. Consultar Odoo para el lote actual (Paralelo controlado)
        semaphore = asyncio.Semaphore(CONCURRENCY_LOTE)
        async with httpx.AsyncClient() as client:
            async def fetch_worker(u):
                async with semaphore:
                    res = await odoo.get_status_async(client, u['username'])
                    return res, u

            tasks = [fetch_worker(u) for u in lote]
            lote_resultados = await asyncio.gather(*tasks)

        # 2. Aplicar cambios a la BD para este lote
        with get_db() as db:
            modificados_lote = 0
            for res, db_user in lote_resultados:
                if res['state'] is None: continue
                
                objetivo = res['iptv_username'] if res['iptv_username'] else res['username']
                iptv_is_active = (db_user['enabled'] == 1)
                odoo_is_active = res['active']
                
                inconsistencia = False
                alerta = ""

                # Limpieza de notas originales
                original_notes = db_user.get('admin_notes', '') or ""
                if "Odoo:" in original_notes:
                    partes = [p.strip() for p in original_notes.split("|") if "Odoo:" not in p and "Alert:" not in p]
                    original_notes = " | ".join(partes)

                if iptv_is_active and not odoo_is_active:
                    inconsistencia = True
                    alerta = "ALERTA_FANTASMA" if res['state'] == 'not_found' else "CORTE_FORZADO"
                elif not iptv_is_active and odoo_is_active:
                    inconsistencia = True
                    alerta = "ALTA_FORZADA"

                # Construir nueva nota
                nueva_tag = f"Odoo:{res['state'].capitalize()}"
                if alerta: nueva_tag += f" | Alert: {alerta}"
                final_note = f"{nueva_tag} | {original_notes}" if original_notes else nueva_tag
                
                if habilitado:
                    if odoo_is_active:
                        if inconsistencia or final_note != db_user.get('admin_notes'):
                            db.execute(
                                text("UPDATE users SET enabled = 1, admin_enabled = 1, exp_date = :exp, admin_notes = :nota WHERE username = :user"),
                                {"exp": ACTIVE_EXP_DATE, "nota": final_note, "user": objetivo}
                            )
                            modificados_lote += 1
                    else:
                        deberia_cortar = (res['state'] != 'not_found')
                        if inconsistencia or final_note != db_user.get('admin_notes'):
                            if deberia_cortar:
                                db.execute(text("UPDATE users SET enabled = 0, admin_notes = :nota WHERE username = :user"), {"nota": final_note, "user": objetivo})
                            else:
                                db.execute(text("UPDATE users SET admin_notes = :nota WHERE username = :user"), {"nota": final_note, "user": objetivo})
                            modificados_lote += 1
            
            if habilitado:
                db.commit()
            
            logger.info(f"Lote {i//BATCH_SIZE + 1} completado. Procesados: {min(i + BATCH_SIZE, len(usuarios))}/{len(usuarios)}")
            
    logger.info("== CICLO DE SINCRONIZACIÓN FINALIZADO ==")

def sync_wrapper():
    """Wrapper para ejecutar la corrutina en el scheduler de APScheduler."""
    asyncio.run(run_sync_logic())

# --- API Y TAREAS ---
scheduler = BackgroundScheduler()
scheduler.add_job(sync_wrapper, 'interval', minutes=30, id="job_sync")
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
    uvicorn.run(app, host="0.0.0.0", port=8016)
