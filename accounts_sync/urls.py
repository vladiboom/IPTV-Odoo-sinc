from django.urls import path
from . import views

urlpatterns = [
    # Vista principal del Dashboard
    path('', views.dashboard_index, name='dashboard_index'),
    # Endpoint para forzar sincronización manual
    path('manual-sync/', views.trigger_manual_sync, name='manual_sync'),
]
