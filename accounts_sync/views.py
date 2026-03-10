import os
import requests
import pymysql
import datetime
import time
import logging
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from dotenv import load_dotenv
from contextlib import contextmanager

# Configuración de registros (logs) para Django
logger = logging.getLogger(__name__)
load_dotenv()

@contextmanager
def mysql_conn():
    """Context manager para asegurar el cierre de la conexión MySQL y manejar errores."""
    conn = None
    try:
        conn = pymysql.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASS", "").strip('\"'),
            database=os.getenv("MYSQL_DB"),
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=3
        )
        yield conn
    except Exception as e:
        logger.error(f"Error de base de datos en Dashboard: {e}")
        raise Exception("No se pudo conectar a la base de datos de la plataforma IPTV.")
    finally:
        if conn:
            conn.close()

@login_required
def dashboard_index(request):
    """Vista principal: Muestra la auditoría de sincronización entre Odoo e IPTV."""
    usuarios = []
    estadisticas = {"total": 0, "sincronizados": 0, "discrepancias": 0}
    ahora_ts = int(time.time())
    
    # Traducción amigable de los estados técnicos de Odoo
    traducciones_odoo = {
        'active': 'Activo',
        'inactive': 'Inactivo',
        'disabled': 'Cortado',
        'suspended': 'Suspendido',
        'cancel': 'Anulado',
        'removal_list': 'Lista de Retiro'
    }

    try:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                # Obtenemos los últimos 100 registros para auditar (ahora incluyendo sync_alert)
                cursor.execute("""
                    SELECT id, username, enabled, admin_enabled, exp_date, admin_notes 
                    FROM users 
                    ORDER BY id DESC 
                    LIMIT 200
                """)
                usuarios = cursor.fetchall()
                
                for usr in usuarios:
                    # 1. Recuperar el estado de Odoo (Almacenado por el puente en admin_notes)
                    notas = usr.get('admin_notes', '')
                    raw_odoo = ""
                    if notas and "Odoo:" in notas:
                        # Extraer solo la parte del estado (antes del pipe si existe)
                        # Ejemplo: "Odoo:active | Alert: ..." -> "active"
                        partes = notas.split("Odoo:")[-1].split("|")
                        raw_odoo = partes[0].strip().lower()
                        usr['odoo_state'] = traducciones_odoo.get(raw_odoo, raw_odoo.capitalize())
                        usr['odoo_color'] = "emerald" if raw_odoo == 'active' else "rose"
                    else:
                        usr['odoo_state'] = "Desconocido"
                        usr['odoo_color'] = "slate"

                    # 2. Evaluar estado real en IPTV (Considerando habilitación y fecha de expiración)
                    habilitado = usr.get('enabled') == 1
                    exp_ts = int(usr['exp_date']) if usr.get('exp_date') and str(usr['exp_date']).isdigit() else 0
                    vencido = (exp_ts < ahora_ts and exp_ts != 0)
                    iptv_activo = habilitado and not vencido
                    
                    usr['iptv_label'] = "HABILITADO" if iptv_activo else "SIN ACCESO"
                    usr['iptv_color'] = "emerald" if iptv_activo else "rose"

                    # 3. Cálculo de Sincronía y Manejo de Inconsistencias
                    # Detectamos si hay alerta guardada en la nota (ej. "Odoo:Active | Alert: CORTE_FORZADO")
                    sync_alert = ""
                    if "| Alert:" in notas:
                        sync_alert = notas.split("| Alert:")[-1].strip()
                    
                    usr['sync_alert'] = sync_alert
                    
                    if not raw_odoo:
                        usr['sync_status'] = "PENDIENTE"
                    else:
                        quiere_activo = (raw_odoo == 'active')
                        
                        if sync_alert:
                            usr['sync_status'] = "INCONSISTENCIA CORREGIDA"
                            estadisticas["discrepancias"] += 1
                        elif quiere_activo == iptv_activo:
                            usr['sync_status'] = "SINCRONIZADO"
                            estadisticas["sincronizados"] += 1
                        else:
                            usr['sync_status'] = "DISCREPANCIA"
                            estadisticas["discrepancias"] += 1

                    # Fecha formateada
                    usr['exp_date_human'] = datetime.datetime.fromtimestamp(exp_ts).strftime('%d/%m/%Y') if exp_ts != 0 else "Ilimitado"
                    estadisticas["total"] += 1
                    
    except Exception as e:
        messages.error(request, str(e))

    return render(request, 'accounts_sync/index.html', {
        'accounts': usuarios,
        'stats': estadisticas,
        'last_update': datetime.datetime.now().strftime('%H:%M:%S'),
        'dry_run': os.getenv("SYNC_ENABLED", "false").lower() == "false"
    })

@login_required
def trigger_manual_sync(request):
    """Punto de entrada para forzar la sincronización desde la interfaz web."""
    api_host = os.getenv("SYNC_API_HOST", "http://127.0.0.1:8016")
    try:
        # Llamada al microservicio de sincronización (ahora en puerto 8016 por defecto)
        requests.post(f"{api_host}/sync-now", timeout=5)
        messages.success(request, "El motor de sincronización ha sido notificado.")
    except Exception as e:
        logger.error(f"Fallo al disparar sync manual: {e}")
        messages.error(request, "No se pudo conectar con el motor de sincronización.")
    
    return redirect('dashboard_index')
