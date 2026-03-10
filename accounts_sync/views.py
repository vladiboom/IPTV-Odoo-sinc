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

from django.core.paginator import Paginator

@login_required
def dashboard_index(request):
    """Vista principal: Muestra la auditoría de sincronización entre Odoo e IPTV."""
    ahora_ts = int(time.time())
    page_number = request.GET.get('page', 1)
    status_filter = request.GET.get('status', 'all')
    
    traducciones_odoo = {
        'active': 'Activo', 'inactive': 'Inactivo', 'disabled': 'Cortado',
        'suspended': 'Suspendido', 'cancel': 'Anulado', 'removal_list': 'Lista de Retiro',
        'not_found': 'No Existe'
    }

    estadisticas = {"total": 0, "sincronizados": 0, "discrepancias": 0}
    usuarios = []

    try:
        with mysql_conn() as conn:
            with conn.cursor() as cursor:
                # 1. Obtener Estadísticas Globales Rápida (Sin recorrer todo en Python)
                cursor.execute("SELECT COUNT(*) as total FROM users")
                estadisticas["total"] = cursor.fetchone()["total"]
                
                cursor.execute("SELECT COUNT(*) as disc FROM users WHERE admin_notes LIKE '%Alert:%'")
                estadisticas["discrepancias"] = cursor.fetchone()["disc"]
                
                # Sincronizados (Estimación rápida: los que tienen Odoo:Active y están habilitados sin alerta)
                cursor.execute("SELECT COUNT(*) as ok FROM users WHERE enabled = 1 AND admin_notes LIKE 'Odoo:Active%' AND admin_notes NOT LIKE '%Alert:%'")
                estadisticas["sincronizados"] = cursor.fetchone()["ok"]

                # 2. Construir Query con Filtro
                base_query = "SELECT id, username, enabled, exp_date, admin_notes FROM users"
                params = []
                if status_filter == 'activo':
                    base_query += " WHERE enabled = 1"
                elif status_filter == 'cortado':
                    base_query += " WHERE enabled = 0"
                
                base_query += " ORDER BY id DESC"
                
                cursor.execute(base_query, params)
                raw_users = cursor.fetchall()

                # 3. Procesar datos para la vista
                for usr in raw_users:
                    notas = usr.get('admin_notes', '') or ""
                    raw_odoo = ""
                    if "Odoo:" in notas:
                        partes = notas.split("Odoo:")[-1].split("|")
                        raw_odoo = partes[0].strip().lower()
                        usr['odoo_state'] = traducciones_odoo.get(raw_odoo, raw_odoo.capitalize())
                        usr['odoo_color'] = "emerald" if raw_odoo == 'active' else "rose"
                    else:
                        usr['odoo_state'] = "Esperando..."
                        usr['odoo_color'] = "slate"

                    habilitado = usr.get('enabled') == 1
                    usr['iptv_label'] = "ACTIVO" if habilitado else "CORTADO"
                    usr['iptv_color'] = "emerald" if habilitado else "rose"

                    sync_alert = ""
                    if "| Alert:" in notas:
                        sync_alert = notas.split("| Alert:")[-1].strip()
                    usr['sync_alert'] = sync_alert
                    
                    if not raw_odoo:
                        usr['sync_status'] = "PENDIENTE"
                    elif sync_alert:
                        usr['sync_status'] = "DISCREPANCIA"
                    elif (raw_odoo == 'active') == habilitado:
                        usr['sync_status'] = "SINCRONIZADO"
                    else:
                        usr['sync_status'] = "DISCREPANCIA"
                
                usuarios = raw_users

    except Exception as e:
        logger.error(f"Error cargando dashboard: {e}")
        messages.error(request, "Error al cargar datos del servidor.")

    # 4. Paginación (100 por página)
    paginator = Paginator(usuarios, 100)
    page_obj = paginator.get_page(page_number)

    return render(request, 'accounts_sync/index.html', {
        'page_obj': page_obj,
        'stats': estadisticas,
        'status_filter': status_filter,
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
