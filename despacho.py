import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import urllib.parse
import requests
import io
import re
import plotly.express as px
from docx import Document
from docx.shared import Inches

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Supervisión Despacho - SEIN", layout="wide", initial_sidebar_state="expanded")
st.title("⚡ Dashboard de Supervisión - Despacho Ejecutado del SEIN ")
st.markdown("Supervisión del Despacho, Interconexiones y Seguridad Operativa")

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Setiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

# --- CARGAR MATRIZ DE CENTRALES DEL SEIN ---
@st.cache_data
def cargar_centrales_sein():
    try:
        # Se ancla la extracción exactamente a las columnas A, B, C, D, E, G, H, I 
        df_centrales = pd.read_excel('CetralesSEIN.xlsx', sheet_name=0, header=None, usecols=[0, 1, 2, 3, 4, 6, 7, 8])
        
        # Saltar fila de encabezado (fila 0)
        df_centrales_limpio = df_centrales.iloc[1:].copy()
        
        df_centrales_limpio.columns = [
            'CODIGO', 'CENTRAL', 'CENTRAL_CALIFICACION', 'EMPRESA_DESPACHO', 
            'AREA_OPERATIVA', 'TIPO_INTEGRANTE', 'TIPO_GENERACION', 'REQUERIMIENTO_ESPECIAL'
        ]
        
        # Limpiar espacios y celdas nulas
        for col in df_centrales_limpio.columns:
            df_centrales_limpio[col] = df_centrales_limpio[col].apply(lambda x: str(x).strip() if pd.notna(x) and str(x) != 'nan' else '')
        
        # Filtrar solo registros donde el nombre de la central sea válido
        df_centrales_limpio = df_centrales_limpio[df_centrales_limpio['CENTRAL'] != ''].copy()
        
        return df_centrales_limpio
    except Exception as e:
        st.error(f"Error cargando CetralesSEIN.xlsx: {e}")
        return pd.DataFrame()

# Cargar al iniciar
df_matriz_centrales = cargar_centrales_sein()

# Crear un diccionario global de tipos de recursos basados en la Columna H
if not df_matriz_centrales.empty:
    dict_recursos_maestro = dict(zip(
        df_matriz_centrales['CENTRAL'].str.strip().str.upper(), 
        df_matriz_centrales['TIPO_GENERACION'].str.strip().str.upper()
    ))
else:
    dict_recursos_maestro = {}

# --- 2. FUNCIONES DE EXTRACCIÓN Y LIMPIEZA (ETL) ---
def generar_urls_coes(fecha):
    año = fecha.strftime("%Y")
    mes_num = fecha.strftime("%m")
    dia = fecha.strftime("%d")
    mes_titulo = MESES[fecha.month]
    fecha_str = fecha.strftime("%d%m")
    
    path_nuevo = f"Post Operación/Reportes/IEOD/{año}/{mes_num}_{mes_titulo}/{dia}/AnexoA_{fecha_str}.xlsx"
    path_legacy = f"Post Operación/Reportes/IEOD/{año}/{mes_num}_{mes_titulo}/{dia}/Anexo1_Resumen_{fecha_str}.xlsx"
    
    return [
        (f"https://www.coes.org.pe/portal/browser/download?url={urllib.parse.quote(path_nuevo)}", "AnexoA"),
        (f"https://www.coes.org.pe/portal/browser/download?url={urllib.parse.quote(path_legacy)}", "Anexo1")
    ]

def parse_dates_coes(val, base_date):
    if pd.isna(val): return pd.NaT
    if isinstance(val, (datetime, pd.Timestamp)): return pd.to_datetime(val)
    try:
        parsed = pd.to_datetime(str(val))
        if parsed.year == datetime.now().year and parsed.month == datetime.now().month and parsed.day == datetime.now().day:
            return pd.Timestamp(datetime.combine(base_date.date(), parsed.time()))
        return parsed
    except:
        return pd.NaT

@st.cache_data(show_spinner=False)
def extraer_datos_despacho(fecha, dict_recursos):
    urls = generar_urls_coes(fecha)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    errores_tecnicos = []
    df_melt = pd.DataFrame()
    df_inter_melt = pd.DataFrame()
    df_seguridad_dia = pd.DataFrame()
    df_demanda_dia = pd.DataFrame()
    
    for url, tipo_anexo in urls:
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                try:
                    archivo_excel = io.BytesIO(res.content)
                    xls = pd.ExcelFile(archivo_excel, engine='openpyxl')
                except Exception as e:
                    errores_tecnicos.append(f"{tipo_anexo}: El archivo en la URL no es un Excel válido ({type(e).__name__})")
                    continue
                    
                hojas_limpias = {h.strip().upper(): h for h in xls.sheet_names}
                
                # ==========================================
                # EXTRACCIÓN HOJA 1: DESPACHO_EJECUTADO
                # ==========================================
                if "DESPACHO_EJECUTADO" in hojas_limpias:
                    nombre_real = hojas_limpias["DESPACHO_EJECUTADO"]
                    df_raw = pd.read_excel(xls, sheet_name=nombre_real, header=None)
                    
                    if tipo_anexo == "AnexoA":
                        zonas_raw = df_raw.iloc[6, 2:].values
                        tipos_raw = df_raw.iloc[7, 2:].values
                        empresas_raw = df_raw.iloc[8, 2:].values
                        plantas_raw = df_raw.iloc[9, 2:].values
                        data_raw = df_raw.iloc[10:58, 2:].values
                    else:
                        plantas_raw = df_raw.iloc[9, 1:].values
                        zonas_raw = df_raw.iloc[6, 1:1 + len(plantas_raw)].values
                        tipos_raw = ["N/A"] * len(plantas_raw)
                        empresas_raw = ["N/A"] * len(plantas_raw)
                        data_raw = df_raw.iloc[10:58, 1:1 + len(plantas_raw)].values
                    
                    idx_validos, nombres_plantas, dict_metadatos = [], [], {}
                    
                    for i, p in enumerate(plantas_raw):
                        if pd.notna(p):
                            nombre_base_crudo = str(p).strip().upper()
                            
                            if nombre_base_crudo != '' and 'MW' not in nombre_base_crudo:
                                tipo_excel = str(tipos_raw[i]).strip().upper() if pd.notna(tipos_raw[i]) else "N/A"
                                
                                # Buscar el tipo exacto en la Columna H de la Matriz SEIN
                                tipo_real = dict_recursos.get(nombre_base_crudo, tipo_excel)
                                
                                # Solo aplicamos fallback si el maestro no tiene info detallada
                                if tipo_real in ["N/A", "TERMOELÉCTRICA", "TERMOELECTRICA"]:
                                    centrales_biomasa = ["MAPLE ETANOL", "SAN JACINTO", "AGROLMOS", "CAÑA BRAVA", "CASA GRANDE"]
                                    centrales_diesel = ["TUMBES MAK 1", "TUMBES MAK 2", "RF GENERACION TALARA", "RECKA", "RF ETEN TG1", "RF ETEN TG2"]
                                    centrales_gas = ["MALACAS1 TG6", "MALACAS2 TG4", "REFINERIA TALARA TV1", "REFINERÍA TALARA TV2"]
                                    
                                    if nombre_base_crudo in centrales_biomasa:
                                        tipo_real = "BIOMASA"
                                    elif nombre_base_crudo in centrales_diesel:
                                        tipo_real = "DIESEL/RESIDUAL"
                                    elif nombre_base_crudo in centrales_gas:
                                        tipo_real = "GAS"
                                
                                # Preservar el agrupamiento exacto del Excel
                                tipo_agrupado = tipo_real

                                # Determinar abreviatura para sufijo de central
                                if "BIOMASA" in tipo_real: abrev = "BIO"
                                elif "GAS" in tipo_real: abrev = "GAS"
                                elif "DIESEL" in tipo_real or "RESIDUAL" in tipo_real: abrev = "DIE"
                                elif "HIDRO" in tipo_real: abrev = "HID"
                                elif "SOLAR" in tipo_real: abrev = "SOL"
                                elif "EOL" in tipo_real or "EÓL" in tipo_real: abrev = "EOL"
                                else: abrev = tipo_real[:3]

                                nombre_central = f"{nombre_base_crudo} ({abrev})"
                                
                                idx_validos.append(i)
                                nombres_plantas.append(nombre_central)
                                dict_metadatos[nombre_central] = {
                                    'ZONA': str(zonas_raw[i]).strip().upper() if pd.notna(zonas_raw[i]) else "N/A",
                                    'TIPO_CENTRAL': tipo_agrupado,
                                    'EMPRESA': str(empresas_raw[i]).strip().upper() if pd.notna(empresas_raw[i]) else "N/A"
                                }
                    
                    datos_limpios = data_raw[:, idx_validos]
                    df_dia_des = pd.DataFrame(datos_limpios, columns=nombres_plantas)
                    fechas_horas = [fecha + timedelta(minutes=30 * (i + 1)) for i in range(48)]
                    df_dia_des['FECHA_HORA'] = fechas_horas
                    
                    df_melt = df_dia_des.melt(id_vars=['FECHA_HORA'], var_name='CENTRAL', value_name='DESPACHO_MW')
                    df_melt['DESPACHO_MW'] = pd.to_numeric(df_melt['DESPACHO_MW'], errors='coerce').fillna(0)
                    df_melt['ZONA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['ZONA'])
                    df_melt['TIPO_CENTRAL'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['TIPO_CENTRAL'])
                    df_melt['EMPRESA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['EMPRESA'])
                    df_melt = df_melt.groupby(['FECHA_HORA', 'CENTRAL', 'ZONA', 'TIPO_CENTRAL', 'EMPRESA'], as_index=False)['DESPACHO_MW'].sum()
                
                # ==========================================
                # EXTRACCIÓN HOJA: DEMANDA_AREAS
                # ==========================================
                if "DEMANDA_AREAS" in hojas_limpias:
                    try:
                        hoja_dem = hojas_limpias["DEMANDA_AREAS"]
                        df_raw_dem = pd.read_excel(xls, sheet_name=hoja_dem, header=None)
                        data_dem = df_raw_dem.iloc[7:55, 4].values
                        df_demanda_dia = pd.DataFrame({
                            'FECHA_HORA': [fecha + timedelta(minutes=30 * (i + 1)) for i in range(48)],
                            'DEMANDA_MW': data_dem
                        })
                        df_demanda_dia['DEMANDA_MW'] = pd.to_numeric(df_demanda_dia['DEMANDA_MW'], errors='coerce').fillna(0)
                    except Exception as e_dem:
                        pass
                        
                # ==========================================
                # EXTRACCIÓN HOJA: INTERCONEXIONES
                # ==========================================
                if "INTERCONEXIONES" in hojas_limpias:
                    try:
                        hoja_inter = hojas_limpias["INTERCONEXIONES"]
                        df_raw_inter = pd.read_excel(xls, sheet_name=hoja_inter, header=None)
                        nombres_lineas = df_raw_inter.iloc[6, 2:7].values
                        nombres_lineas = [str(x).strip() if pd.notna(x) else f"LÍNEA_{idx+1}" for idx, x in enumerate(nombres_lineas)]
                        data_inter = df_raw_inter.iloc[7:55, 2:7].values
                        df_inter_dia = pd.DataFrame(data_inter, columns=nombres_lineas)
                        df_inter_dia['FECHA_HORA'] = [fecha + timedelta(minutes=30 * (i + 1)) for i in range(48)]
                        df_inter_melt = df_inter_dia.melt(id_vars=['FECHA_HORA'], var_name='LINEA_TRANSMISION', value_name='FLUJO_MW')
                        df_inter_melt['FLUJO_MW'] = pd.to_numeric(df_inter_melt['FLUJO_MW'], errors='coerce').fillna(0)
                    except Exception as e_inter:
                        pass

                # ==========================================
                # EXTRACCIÓN HOJA: CALIFICA_OPE_UG (CALIFICACIÓN DE OPERACIONES)
                # ==========================================
                if "CALIFICA_OPE_UG" in hojas_limpias:
                    try:
                        hoja_cal = hojas_limpias["CALIFICA_OPE_UG"]
                        df_raw_cal = pd.read_excel(xls, sheet_name=hoja_cal, header=None)
                        df_califica = df_raw_cal.iloc[6:, [1, 2, 3, 4, 5, 6, 9]].copy()
                        df_califica.columns = ['EMPRESA', 'CENTRAL', 'GRUPO', 'MODO_OPERACION', 'INICIO', 'FIN', 'TIPO_OPERACION']
                        df_califica = df_califica.dropna(how='all')
                        df_califica['TIPO_OPERACION'] = df_califica['TIPO_OPERACION'].astype(str).str.upper().str.strip()
                        
                        df_seguridad_dia = df_califica.copy()
                        if not df_seguridad_dia.empty:
                            df_seguridad_dia['INICIO'] = df_seguridad_dia['INICIO'].apply(lambda x: parse_dates_coes(x, fecha))
                            df_seguridad_dia['FIN'] = df_seguridad_dia['FIN'].apply(lambda x: parse_dates_coes(x, fecha))
                            df_seguridad_dia['FECHA_REPORTE'] = fecha.date()
                    except Exception as e_cal:
                        pass

                return df_melt, df_inter_melt, df_seguridad_dia, df_demanda_dia, None
                
            else:
                errores_tecnicos.append(f"{tipo_anexo}: HTTP {res.status_code}")
                
        except Exception as e:
            errores_tecnicos.append(f"Error nativo en {tipo_anexo}: {type(e).__name__} - {str(e)}")
            continue
            
    motivo_error = " | ".join(errores_tecnicos) if errores_tecnicos else "Falla desconocida."
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] ❌ Motivo: {motivo_error}"

def procesar_rango_fechas(start_date, end_date, progress_bar, status_text, dict_recursos):
    fechas = pd.date_range(start_date, end_date)
    total_dias = len(fechas)
    lista_dfs, lista_inter, lista_seguridad, lista_demanda, alertas = [], [], [], [], []
    
    for i, f in enumerate(fechas):
        status_text.markdown(f"**⏳ Sincronizando datos de Despacho (COES):** {f.strftime('%d/%m/%Y')} *(Día {i+1} de {total_dias})*")
        df_dia, df_inter_dia, df_seguridad_dia, df_demanda_dia, error = extraer_datos_despacho(f, dict_recursos)
        
        if not df_dia.empty: lista_dfs.append(df_dia)
        if not df_inter_dia.empty: lista_inter.append(df_inter_dia)
        if not df_seguridad_dia.empty: lista_seguridad.append(df_seguridad_dia)
        if not df_demanda_dia.empty: lista_demanda.append(df_demanda_dia)
        if error: alertas.append(error)
            
        progress_bar.progress((i + 1) / total_dias)
            
    df_final = pd.concat(lista_dfs, ignore_index=True) if lista_dfs else pd.DataFrame()
    df_inter_final = pd.concat(lista_inter, ignore_index=True) if lista_inter else pd.DataFrame()
    df_seguridad_final = pd.concat(lista_seguridad, ignore_index=True) if lista_seguridad else pd.DataFrame()
    df_demanda_final = pd.concat(lista_demanda, ignore_index=True) if lista_demanda else pd.DataFrame()
    
    return df_final, df_inter_final, df_seguridad_final, df_demanda_final, alertas

# --- 3. INTERFAZ DE USUARIO ---
st.sidebar.header("⚙️ Parámetros del Dashboard SEIN")
hoy = datetime.now().date()
rango_fechas = st.sidebar.date_input("Intervalo de Fechas", value=(hoy,hoy))

if st.sidebar.button("📊 Extraer Información - IEOD ", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        start_date, end_date = rango_fechas
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        df_consolidado, df_inter_consolidado, df_seg_consolidado, df_dem_consolidado, alertas = procesar_rango_fechas(start_date, end_date, progress_bar, status_text, dict_recursos_maestro)
        
        st.session_state['df_despacho'] = df_consolidado
        st.session_state['df_interconexiones'] = df_inter_consolidado
        st.session_state['df_seguridad'] = df_seg_consolidado
        st.session_state['df_demanda'] = df_dem_consolidado
        st.session_state['alertas_despacho'] = alertas
            
        status_text.empty()
        progress_bar.empty()

# --- 4. VISUALIZACIÓN DE DATOS ---
if 'df_despacho' in st.session_state:
    df_raw = st.session_state['df_despacho']
    df_inter_raw = st.session_state.get('df_interconexiones', pd.DataFrame())
    df_seg_raw = st.session_state.get('df_seguridad', pd.DataFrame())
    df_dem_raw = st.session_state.get('df_demanda', pd.DataFrame())
    alertas = st.session_state['alertas_despacho']
    
    if df_raw.empty:
        st.error("🚨 Extracción fallida o sin datos operacionales.")
    elif df_matriz_centrales.empty:
        st.error("❌ No se pudo cargar la matriz de centrales CetralesSEIN.xlsx")
    else:
        # ==========================================
        # FILTROS DINÁMICOS EN CASCADA
        # ==========================================
        st.sidebar.markdown("---")
        st.sidebar.header("🔍 Filtros Operativos")
        
        # Filtro 1: Área
        opts_zona = sorted([x for x in df_matriz_centrales['AREA_OPERATIVA'].unique() if x])
        filtro_zona = st.sidebar.multiselect("📍 Área Operativa:", options=opts_zona, placeholder="Todas")
        df_f1 = df_matriz_centrales[df_matriz_centrales['AREA_OPERATIVA'].isin(filtro_zona)] if filtro_zona else df_matriz_centrales

        # Filtro 2: Tipo Integrante (COES/NO COES)
        opts_int = sorted([x for x in df_f1['TIPO_INTEGRANTE'].unique() if x])
        defecto_int = ["COES"] if "COES" in opts_int else []
        filtro_int = st.sidebar.multiselect("⚖️ Tipo Integrante:", options=opts_int, default=defecto_int, placeholder="Todos")
        df_f2 = df_f1[df_f1['TIPO_INTEGRANTE'].isin(filtro_int)] if filtro_int else df_f1
        
        # Filtro 3: Req. Especial
        opts_req = sorted([x for x in df_f2['REQUERIMIENTO_ESPECIAL'].unique() if x])
        filtro_req = st.sidebar.multiselect("⚠️ Req. Especial:", options=opts_req, placeholder="Todos")
        df_f3 = df_f2[df_f2['REQUERIMIENTO_ESPECIAL'].isin(filtro_req)] if filtro_req else df_f2

        # Filtro 4: Empresa
        opts_emp = sorted([x for x in df_f3['EMPRESA_DESPACHO'].unique() if x])
        filtro_empresa = st.sidebar.multiselect("🏢 Empresa:", options=opts_emp, placeholder="Todas")
        df_f4 = df_f3[df_f3['EMPRESA_DESPACHO'].isin(filtro_empresa)] if filtro_empresa else df_f3

        # Filtro 5: Tipo de Generación (Basado en la Columna H de tu maestro)
        opts_tipo = sorted([x for x in df_f4['TIPO_GENERACION'].unique() if x])
        filtro_tipo = st.sidebar.multiselect("⚡ Tipo de Recurso:", options=opts_tipo, placeholder="Todas")
        df_f5 = df_f4[df_f4['TIPO_GENERACION'].isin(filtro_tipo)] if filtro_tipo else df_f4

        # Filtro 6: Central
        opts_cen = sorted([x for x in df_f5['CENTRAL'].unique() if x])
        filtro_cen = st.sidebar.multiselect("🏭 Central:", options=opts_cen, placeholder="Todas")
        df_f_final = df_f5[df_f5['CENTRAL'].isin(filtro_cen)] if filtro_cen else df_f5

        # Consolidar listas finales para enlace
        centrales_filtradas = df_f_final['CENTRAL'].str.strip().str.upper().tolist()
        nombres_calificacion_activos = [str(n).strip().upper() for n in df_f_final['CENTRAL_CALIFICACION'].unique() if n and n != "N/A" and str(n).lower() != 'nan']

        # Enlace ultra-robusto del Filtro con df_raw
        def es_central_valida(central_name):
            # Elimina sufijos como "(BIO)", "(HID)", "(GAS)" generados en ETL
            nom_limpio = re.sub(r'\s*\([^)]*\)$', '', str(central_name)).strip().upper()
            
            # Validación flexible de coincidencia parcial
            for c_filt in centrales_filtradas:
                if c_filt == nom_limpio or c_filt in nom_limpio or nom_limpio in c_filt:
                    return True
            return False

        df_datos = df_raw[df_raw['CENTRAL'].apply(es_central_valida)]
        
        if df_datos.empty:
            st.warning("⚠️ No hay datos despachados para las centrales filtradas en las fechas seleccionadas.")
        else:
            st.success("✅ Datos mostrados correctamente.")
            
            # Definición centralizada de colores para usar en todo el dashboard (ACTUALIZADO SEGÚN ESPECIFICACIÓN)
            colores_tecnologia = {
                "EÓLICA": "#808080", "EOLICA": "#808080",       # Plomo
                "HIDROELÉCTRICA": "#00BFFF", "HIDROELECTRICA": "#00BFFF", # Celeste
                "SOLAR": "#FFD700",                             # Amarillo
                "DIESEL/RESIDUAL": "#FF0000",                   # Rojo
                "GAS DE LA SELVA": "#90EE90",                   # Verde Claro
                "GAS CAMISEA": "#006400",                       # Verde Oscuro
                "GAS NORTE": "#0bb613",                         # Verde Más Oscuro
                "BIOMASA": "#800080",                           # Púrpura
                "GAS": "#006400"                                # Fallback genérico Verde Oscuro
            }
            
            # --- CONTENEDOR PARA EL BOTÓN DE REPORTE ---
            contenedor_reporte = st.container()
            
            es_formato_anexo1 = (df_datos['EMPRESA'] == 'N/A').all()
            st.markdown("### 📊 Datos Seleccionados (SEIN )")
            
            df_datos['FECHA_DIA'] = df_datos['FECHA_HORA'].dt.date

            if df_datos.empty:
                st.warning("⚠️ No hay datos despachados para la selección actual.")
            else:
                # INICIALIZACIÓN DE VARIABLES PARA EL REPORTE
                fig_tipo = None
                fig_inter = None
                fig_cen = None
                fig_prom = None
                fig_inactividad = None
                fig_actividad = None
                fig_bar_seg = None
                df_bar_data = pd.DataFrame()
                
                # FUNCIÓN HELPER PARA GRÁFICAS DE ÁREA (SIN DEMANDA)
                def crear_grafica_area(df_grafico, col_color, titulo, color_map=None):
                    df_plot = df_grafico.copy().dropna(subset=[col_color])
                    df_plot['DESPACHO_MW'] = pd.to_numeric(df_plot['DESPACHO_MW'], errors='coerce').fillna(0)
                    
                    df_sistema = df_plot.groupby('FECHA_HORA', as_index=False)['DESPACHO_MW'].sum()
                    max_demanda_real = df_sistema['DESPACHO_MW'].max()
                    
                    limite_superior_y = max_demanda_real * 1.05 if pd.notna(max_demanda_real) and max_demanda_real > 0 else 1000
                    fecha_min, fecha_max = df_plot['FECHA_HORA'].min(), df_plot['FECHA_HORA'].max()

                    fig = px.area(
                        df_plot, x="FECHA_HORA", y="DESPACHO_MW", color=col_color, 
                        title=titulo, labels={col_color: "Clasificación"}, 
                        color_discrete_map=color_map,
                        color_discrete_sequence=px.colors.qualitative.Alphabet,
                        template="plotly_white"
                    )
                    
                    fig.update_traces(hovertemplate="%{y:,.2f} MW", line=dict(width=0))
                    fig.add_scatter(
                        x=df_sistema['FECHA_HORA'], y=df_sistema['DESPACHO_MW'], mode='lines',
                        line=dict(width=0, color='rgba(0,0,0,0)'), name='<b>⚡ TOTAL GENERACIÓN</b>',
                        hovertemplate='<b>🗓️ %{x|%d/%m/%Y %H:%M} ➡️ %{y:,.2f} MW</b>', showlegend=False
                    )
                    
                    fig.update_layout(
                        hovermode="x unified",
                        xaxis=dict(tickformat="%d/%m\n%H:%M", title="Fecha Operativa", range=[fecha_min, fecha_max]),
                        yaxis=dict(title="Potencia Activa (MW)", range=[0, limite_superior_y]),
                        height=550, margin=dict(t=50, b=50, l=50, r=20) 
                    )
                    return fig

                # PRECALCULO PARA GRÁFICAS DE UNIDADES
                energia_total_cen = df_datos.groupby('CENTRAL')['DESPACHO_MW'].sum()
                centrales_activas = energia_total_cen[energia_total_cen > 0].index
                
                df_plot_cen = df_datos[df_datos['CENTRAL'].isin(centrales_activas)].copy()
                energia_ordenada_cen = energia_total_cen[centrales_activas].sort_values(ascending=False).index
                df_plot_cen['CENTRAL'] = pd.Categorical(df_plot_cen['CENTRAL'], categories=energia_ordenada_cen, ordered=True)
                df_plot_cen = df_plot_cen.sort_values(['FECHA_HORA', 'CENTRAL'])

                # ==========================================
                # 1. DESPACHO POR TIPO DE GENERACIÓN (SEIN)
                # ==========================================
                st.markdown("---")
                st.header("1. 🏭 Despacho por Tipo de Generación (SEIN)")
                if not es_formato_anexo1:
                    df_tipo = df_datos.groupby(['FECHA_HORA', 'TIPO_CENTRAL'], as_index=False)['DESPACHO_MW'].sum()
                    energia_tipo = df_tipo.groupby('TIPO_CENTRAL')['DESPACHO_MW'].sum().sort_values(ascending=False).index
                    
                    orden_apilamiento = [
                        "BIOMASA", 
                        "SOLAR", 
                        "EÓLICA", "EOLICA", 
                        "HIDROELÉCTRICA", "HIDROELECTRICA", 
                        "GAS NORTE", 
                        "GAS DE LA SELVA", 
                        "GAS CAMISEA", 
                        "DIESEL/RESIDUAL"
                    ]

                    df_tipo['TIPO_CENTRAL'] = pd.Categorical(df_tipo['TIPO_CENTRAL'], categories=orden_apilamiento, ordered=True)
                    df_tipo = df_tipo.sort_values(['FECHA_HORA', 'TIPO_CENTRAL'])

                    fig_tipo = crear_grafica_area(df_tipo, 'TIPO_CENTRAL', "Curva Apilada por Tecnología - SEIN (MW)", color_map=colores_tecnologia)
                    st.plotly_chart(fig_tipo, use_container_width=True)
                else:
                    st.info("La vista por Tipo de Generación requiere el formato AnexoA (con metadatos), el archivo actual no lo contiene.")

                # ==========================================
                # 2. FLUJO DE INTERCONEXIÓN
                # ==========================================
                st.markdown("---")
                st.header("2. 🔌 Flujo de Interconexión")
                
                if df_inter_raw.empty:
                    st.info("No se encontraron datos de interconexión en el periodo descargado.")
                else:
                    df_inter_plot = df_inter_raw.sort_values(['FECHA_HORA', 'LINEA_TRANSMISION']).copy()
                                       
                    fig_inter = px.area(
                        df_inter_plot, 
                        x="FECHA_HORA", 
                        y="FLUJO_MW", 
                        color="LINEA_TRANSMISION",
                        title="Flujos Activos Apilados por Línea de Transmisión (MW)",
                        labels={"LINEA_TRANSMISION": "Línea de Transmisión", "FLUJO_MW": "Flujo de Potencia (MW)"},
                        color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f'],
                        template="plotly_white"
                    )
                    
                    df_inter_total = df_inter_plot.groupby('FECHA_HORA', as_index=False)['FLUJO_MW'].sum()
                    
                    fig_inter.update_traces(hovertemplate="%{y:,.2f} MW", line=dict(width=0))
                    fig_inter.add_scatter(
                        x=df_inter_total['FECHA_HORA'], y=df_inter_total['FLUJO_MW'], mode='lines',
                        line=dict(width=3, color='black', dash='dash'), name='<b>⚡ FLUJO TOTAL</b>',
                        hovertemplate='<b>🗓️ %{x|%d/%m/%Y %H:%M} ➡️ %{y:,.2f} MW</b>'
                    )

                    fig_inter.update_layout(
                        hovermode="x unified", xaxis=dict(tickformat="%d/%m\n%H:%M", title="Fecha Operativa"),
                        height=550, margin=dict(t=50, b=50, l=50, r=20) 
                    )
                    st.plotly_chart(fig_inter, use_container_width=True)
                    
                    with st.expander("Ver Datos de Flujos (Vista Matricial)"):
                        df_inter_pivot = df_inter_plot.copy()
                        df_inter_pivot['FECHA'] = df_inter_pivot['FECHA_HORA'].dt.strftime('%d/%m/%Y')
                        df_inter_pivot['HORA'] = df_inter_pivot['FECHA_HORA'].dt.strftime('%H:%M')
                        
                        df_mat_inter = df_inter_pivot.pivot_table(
                            index=['FECHA', 'HORA'], columns=['LINEA_TRANSMISION'], values='FLUJO_MW', aggfunc='sum'
                        ).round(2).fillna(0)
                        df_mat_inter['TOTAL_FLUJO'] = df_mat_inter.sum(axis=1)
                        st.dataframe(df_mat_inter, use_container_width=True)

                # ==========================================
                # 3. GENERACIÓN SEIN (SIN DEMANDA)
                # ==========================================
                st.markdown("---")
                st.header("3. 📊 Generación del SEIN por Central")
                
                df_plot_cen_aux = df_plot_cen.copy().dropna(subset=['CENTRAL'])
                df_plot_cen_aux['DESPACHO_MW'] = pd.to_numeric(df_plot_cen_aux['DESPACHO_MW'], errors='coerce').fillna(0)
                
                df_sistema_aux = df_plot_cen_aux.groupby('FECHA_HORA', as_index=False)['DESPACHO_MW'].sum()
                max_demanda_real_aux = df_sistema_aux['DESPACHO_MW'].max()
                
                limite_superior_y_aux = max_demanda_real_aux * 1.05 if pd.notna(max_demanda_real_aux) and max_demanda_real_aux > 0 else 1000
                fecha_min_aux, fecha_max_aux = df_plot_cen_aux['FECHA_HORA'].min(), df_plot_cen_aux['FECHA_HORA'].max()

                fig_cen = px.area(
                    df_plot_cen_aux, x="FECHA_HORA", y="DESPACHO_MW", color='CENTRAL', 
                    title="Despacho de Potencia por Unidad - SEIN (MW)", labels={'CENTRAL': "Central"}, 
                    color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'],
                    template="plotly_white"
                )
                
                fig_cen.update_traces(hovertemplate="%{y:,.2f} MW", line=dict(width=0))
                fig_cen.add_scatter(
                    x=df_sistema_aux['FECHA_HORA'], y=df_sistema_aux['DESPACHO_MW'], mode='lines',
                    line=dict(width=0, color='rgba(0,0,0,0)'), name='<b>⚡ TOTAL GENERACIÓN</b>',
                    hovertemplate='<b>🗓️ %{x|%d/%m/%Y %H:%M} ➡️ %{y:,.2f} MW</b>', showlegend=False
                )
                
                fig_cen.update_layout(
                    hovermode="x unified",
                    xaxis=dict(tickformat="%d/%m\n%H:%M", title="Fecha Operativa", range=[fecha_min_aux, fecha_max_aux]),
                    yaxis=dict(title="Potencia Activa (MW)", range=[0, limite_superior_y_aux]),
                    height=550, margin=dict(t=50, b=50, l=50, r=20) 
                )
                st.plotly_chart(fig_cen, use_container_width=True)

                # ==========================================
                # 4. POTENCIA PROMEDIO DIARIA
                # ==========================================
                st.markdown("---")
                st.header("4. 📈 Potencia Promedio Diaria (SEIN)")
                
                # CORRECCIÓN DE EFECTO DE BORDE: Asignar las 00:00 al día operativo correcto restando 1 minuto
                df_plot_cen['FECHA_DIA_OPERATIVO'] = (df_plot_cen['FECHA_HORA'] - pd.Timedelta(minutes=1)).dt.date
                
                # Gráfica 4.1: Promedio de todo el día (24 Horas / 48 Periodos)
                df_promedio = df_plot_cen.groupby(['FECHA_DIA_OPERATIVO', 'CENTRAL'], as_index=False)['DESPACHO_MW'].mean()
                df_promedio['FECHA_DIA_OPERATIVO'] = pd.to_datetime(df_promedio['FECHA_DIA_OPERATIVO']).dt.strftime('%d/%m/%Y')
                
                fig_prom = px.bar(
                    df_promedio, x='FECHA_DIA_OPERATIVO', y='DESPACHO_MW', color='CENTRAL',
                    title="Potencia Promedio Diaria Total (24 Horas) (MW)",
                    barmode='group',
                    labels={'FECHA_DIA_OPERATIVO': 'Día Operativo', 'DESPACHO_MW': 'Potencia Promedio (MW)'},
                    color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'],
                    template="plotly_white"
                )
                fig_prom.update_layout(xaxis=dict(type='category'), height=500)
                st.plotly_chart(fig_prom, use_container_width=True)

                # Gráfica 4.2: Promedio Operativo (Solo en periodos con inyección > 0 MW)
                df_solo_inyeccion = df_plot_cen[df_plot_cen['DESPACHO_MW'] > 0].copy()
                
                if not df_solo_inyeccion.empty:
                    df_promedio_iny = df_solo_inyeccion.groupby(['FECHA_DIA_OPERATIVO', 'CENTRAL'], as_index=False)['DESPACHO_MW'].mean()
                    df_promedio_iny['FECHA_DIA_OPERATIVO'] = pd.to_datetime(df_promedio_iny['FECHA_DIA_OPERATIVO']).dt.strftime('%d/%m/%Y')
                    
                    fig_prom_iny = px.bar(
                        df_promedio_iny, x='FECHA_DIA_OPERATIVO', y='DESPACHO_MW', color='CENTRAL',
                        title="Potencia Promedio en Operación (MW)",
                        barmode='group',
                        labels={'FECHA_DIA_OPERATIVO': 'Día Operativo', 'DESPACHO_MW': 'Promedio en Operación (MW)'},
                        color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'],
                        template="plotly_white"
                    )
                    fig_prom_iny.update_layout(xaxis=dict(type='category'), height=500)
                    st.plotly_chart(fig_prom_iny, use_container_width=True)
                else:
                    fig_prom_iny = None
                    st.info("No se registraron periodos con inyección de potencia mayor a 0 MW para calcular el promedio operativo.")

                # ==========================================
                # 5. CONTROL DE TIEMPOS: INACTIVIDAD Y OPERACIÓN
                # ==========================================
                st.markdown("---")
                st.header("5. ⏱️ Control de Tiempos: Inactividad y Operación (SEIN)")
                
                df_tiempos = df_datos.copy()
                df_tiempos['INACTIVO_HR'] = (df_tiempos['DESPACHO_MW'] == 0) * 0.5
                df_tiempos['ACTIVO_HR'] = (df_tiempos['DESPACHO_MW'] > 0) * 0.5
                df_resumen_tiempos = df_tiempos.groupby(['CENTRAL', 'TIPO_CENTRAL'], as_index=False)[['INACTIVO_HR', 'ACTIVO_HR']].sum()
                
                # Gráfica 1: Horas No Despachadas
                tipos_inactividad = ['HIDROELÉCTRICA', 'HIDROELECTRICA', 'EÓLICA', 'EOLICA', 'SOLAR', 'BIOMASA', 'GAS DE LA SELVA', 'GAS CAMISEA', 'GAS NORTE']
                df_inactividad = df_resumen_tiempos[df_resumen_tiempos['TIPO_CENTRAL'].isin(tipos_inactividad)].copy()
                
                if not df_inactividad.empty:
                    fig_inactividad = px.bar(
                        df_inactividad, x='CENTRAL', y='INACTIVO_HR', color='TIPO_CENTRAL',
                        title="Horas No Despachadas (Inactividad) por Central",
                        labels={'CENTRAL': 'Central Generadora', 'INACTIVO_HR': 'Horas Inactivas (h)', 'TIPO_CENTRAL': 'Tipo'},
                        color_discrete_map=colores_tecnologia,
                        template="plotly_white"
                    )
                    fig_inactividad.update_layout(xaxis={'categoryorder':'total descending'}, height=450)
                    st.plotly_chart(fig_inactividad, use_container_width=True)
                else:
                    st.info("No hay datos de las tecnologías seleccionadas para inactividad.")

                # Gráfica 2: Horas de Operación (Diesel/Residual)
                tipos_actividad = ['DIESEL/RESIDUAL']
                df_actividad = df_resumen_tiempos[df_resumen_tiempos['TIPO_CENTRAL'].isin(tipos_actividad)].copy()
                
                if not df_actividad.empty:
                    fig_actividad = px.bar(
                        df_actividad, x='CENTRAL', y='ACTIVO_HR', color='TIPO_CENTRAL',
                        title="Horas de Operación (Centrales Diésel/Residual)",
                        labels={'CENTRAL': 'Central Generadora', 'ACTIVO_HR': 'Horas de Operación (h)', 'TIPO_CENTRAL': 'Tipo'},
                        color_discrete_map=colores_tecnologia,
                        template="plotly_white"
                    )
                    fig_actividad.update_layout(xaxis={'categoryorder':'total descending'}, height=450)
                    st.plotly_chart(fig_actividad, use_container_width=True)
                else:
                    st.info("No hay centrales Diésel/Residual operando o en la selección.")

                # ==========================================
                # 6. CALIFICACIÓN DE LA OPERACIÓN (ENLAZADA AL FILTRO)
                # ==========================================
                st.markdown("---")
                st.header("6. 🛡️ Calificación de la Operación")
                
                if df_seg_raw.empty:
                    st.info("No se registraron calificaciones de operación en la hoja CALIFICA_OPE_UG para el periodo consultado.")
                elif not nombres_calificacion_activos:
                    st.warning("Las centrales seleccionadas no poseen mapeo de Calificación de Operación en la matriz.")
                else:
                    df_bar_data = df_seg_raw.dropna(subset=['INICIO', 'FIN']).copy()
                    if not df_bar_data.empty:
                        # ENLACE: Utilizar únicamente las centrales sobrevivientes del filtro en cascada
                        valid_centrales_calif = nombres_calificacion_activos
                        
                        def es_central_valida_calif(nombre):
                            nom_limpio = re.sub(r'\s+', ' ', str(nombre).strip().upper())
                            for valid_c in valid_centrales_calif:
                                if valid_c in nom_limpio:
                                    return True
                            return False
                            
                        df_bar_data = df_bar_data[df_bar_data['CENTRAL'].apply(es_central_valida_calif)].copy()
                        
                        if not df_bar_data.empty:
                            df_bar_data['CENTRAL_GRUPO'] = df_bar_data['CENTRAL'].astype(str) + " - " + df_bar_data['GRUPO'].astype(str)
                            df_bar_data['HORAS_OPERACION'] = (df_bar_data['FIN'] - df_bar_data['INICIO']).dt.total_seconds() / 3600.0
                            df_bar_data['HORAS_OPERACION'] = df_bar_data['HORAS_OPERACION'].clip(lower=0)

                            df_agrupado = df_bar_data.groupby(['CENTRAL_GRUPO', 'TIPO_OPERACION'], as_index=False)['HORAS_OPERACION'].sum()

                            colores_operacion = {
                                "POR SEGURIDAD": "#8B0000",
                                "POR POTENCIA O ENERGIA": "#00BFFF",
                                "A MINIMA CARGA": "#32CD32",
                                "POR COGENERACION": "#FF8C00",
                                "POR RSF": "#FFD700",
                                "POR PRUEBAS": "#808080"
                            }

                            fig_bar_seg = px.bar(
                                df_agrupado,
                                x="HORAS_OPERACION",
                                y="CENTRAL_GRUPO",
                                color="TIPO_OPERACION",
                                orientation='h',
                                title="Horas Totales de Operación por Unidad y Tipo de Operación",
                                labels={
                                    "HORAS_OPERACION": "Horas Totales (h)",
                                    "CENTRAL_GRUPO": "Unidad Generadora",
                                    "TIPO_OPERACION": "Tipo de Operación"
                                },
                                color_discrete_map=colores_operacion,
                                template="plotly_white"
                            )

                            fig_bar_seg.update_layout(
                                yaxis=dict(title="Unidad Generadora", categoryorder="total ascending"),
                                xaxis=dict(title="Horas de Operación Totales"),
                                height=max(400, len(df_agrupado['CENTRAL_GRUPO'].unique()) * 40)
                            )
                            st.plotly_chart(fig_bar_seg, use_container_width=True)

                            with st.expander("Ver Registro Detallado de Calificación de Operaciones"):
                                df_seguridad_filtrado = df_bar_data[df_bar_data['TIPO_OPERACION'] == 'POR SEGURIDAD'].copy()
                                if not df_seguridad_filtrado.empty:
                                    st.dataframe(df_seguridad_filtrado, use_container_width=True)
                                else:
                                    st.info("No hay registros de operación POR SEGURIDAD en este periodo.")
                        else:
                            st.warning("Las centrales filtradas no registraron operaciones calificadas en este periodo.")
                    else:
                        st.warning("Faltan datos válidos de INICIO o FIN para graficar las horas de operación.")

                # ==========================================
                # LÓGICA DE EXPORTACIÓN A WORD (EN EL CONTENEDOR INICIAL)
                # ==========================================
                with contenedor_reporte:
                    if st.button("📄 Preparar Reporte Word (Incluye Gráficos)", use_container_width=True):
                        with st.spinner("Compilando gráficos... (Nota: Este proceso requiere tener instalada la librería 'kaleido')"):
                            doc = Document()
                            doc.add_heading('Dashboard de Supervisión - Despacho SEIN', 0)
                            doc.add_paragraph(f"Reporte generado automáticamente el: {datetime.now().strftime('%d/%m/%Y a las %H:%M')}")

                            # Variable para detectar si falla la captura de imágenes en el servidor/PC
                            error_graficos = False

                            def agregar_grafico(doc, fig, titulo):
                                nonlocal error_graficos
                                if fig is not None:
                                    doc.add_heading(titulo, level=1)
                                    try:
                                        # Se reduce la escala a 1.2 para evitar bloqueos de memoria (Timeout)
                                        img_bytes = fig.to_image(format="png", width=800, height=450, scale=1.2)
                                        imagen_stream = io.BytesIO(img_bytes)
                                        doc.add_picture(imagen_stream, width=Inches(6.0))
                                    except Exception as e:
                                        error_graficos = True
                                        doc.add_paragraph(f"⚠️ [Error al generar la imagen de este gráfico]")

                            def agregar_tabla(doc, df, titulo):
                                if df is not None and not df.empty:
                                    doc.add_heading(titulo, level=1)
                                    table = doc.add_table(rows=1, cols=len(df.columns))
                                    table.style = 'Table Grid'
                                    for i, col in enumerate(df.columns):
                                        table.rows[0].cells[i].text = str(col)
                                    for _, row in df.iterrows():
                                        row_cells = table.add_row().cells
                                        for i, val in enumerate(row):
                                            row_cells[i].text = str(val)

                            agregar_grafico(doc, fig_tipo, "1. Despacho por Tipo de Generación")
                            agregar_grafico(doc, fig_inter, "2. Flujo de Interconexión")
                            agregar_grafico(doc, fig_cen, "3. Generación SEIN por Central")
                            agregar_grafico(doc, fig_prom, "4. Potencia Promedio Diaria")
                            agregar_grafico(doc, fig_inactividad, "5.1. Horas No Despachadas (Inactividad)")
                            agregar_grafico(doc, fig_actividad, "5.2. Horas de Operación (Diésel/Residual)")
                            agregar_grafico(doc, fig_bar_seg, "6. Calificación de la Operación (Horas por Tipo)")

                            if not df_bar_data.empty:
                                columnas_limpias = df_bar_data.drop(columns=['HORAS_OPERACION', 'CENTRAL_GRUPO'], errors='ignore')
                                agregar_tabla(doc, columnas_limpias, "Registro Detallado de Calificación de Operaciones")

                            buffer_docx = io.BytesIO()
                            doc.save(buffer_docx)
                            buffer_docx.seek(0)
                            
                            # Mostrar alerta en la interfaz si falló la exportación de gráficos
                            if error_graficos:
                                st.error("❌ Ocurrió un error al intentar capturar los gráficos. \n\n**Solución obligatoria:** Abre tu terminal de comandos (CMD, PowerShell o VSCode) y ejecuta la instalación del motor de captura:\n\n`pip install kaleido==0.1.0.post1`\n\n*(Nota: Es vital usar esta versión específica para evitar bloqueos en Windows. Reinicia tu app después de instalarlo).*")
                            else:
                                st.success("✅ ¡Reporte compilado con éxito incluyendo los gráficos de supervisión!")

                            st.download_button(
                                label="⬇️ Haz clic aquí para descargar tu Reporte (.docx)",
                                data=buffer_docx.getvalue(),
                                file_name=f"Reporte_Despacho_SEIN_{datetime.now().strftime('%Y%m%d_%H%M')}.docx",
                                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                type="primary",
                                use_container_width=True
                            )

                # ==========================================
                # 7. TRAZABILIDAD (DATA CRUDA)
                # ==========================================
                st.markdown("---")
                st.header("7. 🗄️ Trazabilidad de Potencia (Data Cruda - SEIN)")
                
                with st.expander("Ver Matriz de Despacho de Generación", expanded=False):
                    df_pivot = df_plot_cen.copy()
                    df_pivot['FECHA'] = df_pivot['FECHA_HORA'].dt.strftime('%d/%m/%Y')
                    df_pivot['HORA'] = df_pivot['FECHA_HORA'].dt.strftime('%H:%M')
                    
                    jerarquia_columnas = ['CENTRAL'] if es_formato_anexo1 else ['TIPO_CENTRAL', 'EMPRESA', 'CENTRAL']
                    
                    df_matricial = df_pivot.pivot_table(
                        index=['FECHA', 'HORA'],
                        columns=jerarquia_columnas,
                        values='DESPACHO_MW',
                        aggfunc='sum'
                    ).round(2).fillna(0)
                    
                    st.dataframe(df_matricial, use_container_width=True)
                    
                    buffer_xls = io.BytesIO()
                    with pd.ExcelWriter(buffer_xls, engine='openpyxl') as writer:
                        df_matricial.to_excel(writer, sheet_name='Despacho_SEIN')
                        
                    st.download_button(
                        label="📥 Descargar Vista Matricial (Excel)",
                        data=buffer_xls.getvalue(),
                        file_name=f"matriz_sein_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )