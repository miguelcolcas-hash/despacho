import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import urllib.parse
import requests
import io
import plotly.express as px

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Supervisión Despacho - Osinergmin", layout="wide", initial_sidebar_state="expanded")
st.title("⚡ Dashboard de Supervisión - Despacho Ejecutado (SEIN)")
st.markdown("Fiscalización Dinámica de Curvas de Carga del IEOD del COES - Periodos de 30 min")

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Setiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

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

@st.cache_data(show_spinner=False)
def extraer_datos_despacho(fecha):
    urls = generar_urls_coes(fecha)
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    mapa_lomitas = {
        "PUNTA LOMITAS-I": "PUNTA LOMITAS", "PUNTA LOMITAS-II": "PUNTA LOMITAS",
        "P LOMITAS_EXP-BL1": "PUNTA LOMITAS EXPANSIÓN", "P LOMITAS_EXP-BL2": "PUNTA LOMITAS EXPANSIÓN",
        "PUNTA LOMITAS-BL1": "PUNTA LOMITAS", "PUNTA LOMITAS-BL2": "PUNTA LOMITAS",
        "PUN LOMITAS_EXP-BL1": "PUNTA LOMITAS EXPANSIÓN", "PUN LOMITAS_EXP-BL2": "PUNTA LOMITAS EXPANSIÓN"
    }
    
    for url, tipo_anexo in urls:
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                archivo_excel = io.BytesIO(res.content)
                xls = pd.ExcelFile(archivo_excel, engine='openpyxl')
                hojas_limpias = {h.strip().upper(): h for h in xls.sheet_names}
                
                if "DESPACHO_EJECUTADO" in hojas_limpias:
                    # --- EXTRACCIÓN HOJA 1: DESPACHO_EJECUTADO ---
                    nombre_real = hojas_limpias["DESPACHO_EJECUTADO"]
                    df_raw = pd.read_excel(xls, sheet_name=nombre_real, header=None)
                    
                    if tipo_anexo == "AnexoA":
                        zonas_raw = df_raw.iloc[6, 2:].values
                        tipos_raw = df_raw.iloc[7, 2:].values
                        empresas_raw = df_raw.iloc[8, 2:].values
                        plantas_raw = df_raw.iloc[9, 2:].values
                        data_raw = df_raw.iloc[10:58, 2:].values
                    else:
                        plantas_raw = df_raw.iloc[5, 1:].values
                        data_raw = df_raw.iloc[6:54, 1:].values
                        zonas_raw = ["N/A"] * len(plantas_raw)
                        tipos_raw = ["N/A"] * len(plantas_raw)
                        empresas_raw = ["N/A"] * len(plantas_raw)
                    
                    idx_validos, nombres_plantas, dict_metadatos = [], [], {}
                    
                    for i, p in enumerate(plantas_raw):
                        if pd.notna(p):
                            nombre_base_crudo = str(p).strip().upper()
                            if nombre_base_crudo != '' and 'MW' not in nombre_base_crudo:
                                nombre_base = mapa_lomitas.get(nombre_base_crudo, nombre_base_crudo)
                                tipo_original = str(tipos_raw[i]).strip().upper() if pd.notna(tipos_raw[i]) else "N/A"
                                
                                if nombre_base in ["PUNTA LOMITAS", "PUNTA LOMITAS EXPANSIÓN"] and tipo_original == "N/A":
                                    tipo_original = "EOLICA"
                                
                                if tipo_original != "N/A":
                                    if "TERMO" in tipo_original: abrev = "TER"
                                    elif "HIDRO" in tipo_original: abrev = "HID"
                                    elif "SOLAR" in tipo_original: abrev = "SOL"
                                    elif "EOL" in tipo_original or "EÓL" in tipo_original: abrev = "EOL"
                                    else: abrev = tipo_original[:3]
                                    nombre_central = f"{nombre_base} ({abrev})"
                                else:
                                    nombre_central = nombre_base
                                
                                idx_validos.append(i)
                                nombres_plantas.append(nombre_central)
                                dict_metadatos[nombre_central] = {
                                    'ZONA': str(zonas_raw[i]).strip().upper() if pd.notna(zonas_raw[i]) else "N/A",
                                    'TIPO_CENTRAL': tipo_original,
                                    'EMPRESA': str(empresas_raw[i]).strip().upper() if pd.notna(empresas_raw[i]) else "N/A"
                                }
                    
                    datos_limpios = data_raw[:, idx_validos]
                    df_dia = pd.DataFrame(datos_limpios, columns=nombres_plantas)
                    fechas_horas = [fecha + timedelta(minutes=30 * (i + 1)) for i in range(48)]
                    df_dia['FECHA_HORA'] = fechas_horas
                    
                    df_melt = df_dia.melt(id_vars=['FECHA_HORA'], var_name='CENTRAL', value_name='DESPACHO_MW')
                    df_melt['DESPACHO_MW'] = pd.to_numeric(df_melt['DESPACHO_MW'], errors='coerce').fillna(0)
                    df_melt['ZONA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['ZONA'])
                    df_melt['TIPO_CENTRAL'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['TIPO_CENTRAL'])
                    df_melt['EMPRESA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['EMPRESA'])
                    df_melt = df_melt.groupby(['FECHA_HORA', 'CENTRAL', 'ZONA', 'TIPO_CENTRAL', 'EMPRESA'], as_index=False)['DESPACHO_MW'].sum()

                    # --- EXTRACCIÓN HOJA 2: TIPO_RECURSO (LECTURA DINÁMICA Y NORMALIZADA) ---
                    df_recurso_melt = pd.DataFrame()
                    if tipo_anexo == "AnexoA" and "TIPO_RECURSO" in hojas_limpias:
                        hoja_rec = hojas_limpias["TIPO_RECURSO"]
                        df_raw_rec = pd.read_excel(xls, sheet_name=hoja_rec, header=None)
                        
                        # 1. Extraer las cabeceras reales desde la fila 6 (índice 5)
                        cabeceras_crudas = df_raw_rec.iloc[5, 2:15].values
                        
                        # 2. Funciones de limpieza y clasificación robusta
                        def normalizar_texto(texto):
                            if pd.isna(texto): return ""
                            # Convertir a mayúsculas y quitar espacios en los extremos
                            t = str(texto).strip().upper()
                            # Reemplazar tildes para estandarizar la evaluación
                            t = t.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
                            # Colapsar múltiples espacios internos (o saltos de línea) en un solo espacio
                            t = " ".join(t.split())
                            return t

                        def clasificar_recurso(nombre):
                            n = normalizar_texto(nombre)
                            # Evaluamos sobre la cadena ya limpia y mapeamos la nomenclatura exacta del COES
                            
                            # 1. Hidráulica (Pasada y Regulación)
                            if "H. PASADA" in n or "H. REGULACION" in n or "HID" in n: 
                                return 'Hidraulica'
                            
                            # 2. Gas Natural (Camisea vs Norte/Selva)
                            elif "CAMISEA" in n: 
                                return 'Gas de Camisea'
                            elif "MALACAS" in n or "AGUAYTIA" in n or "NORTE" in n or "SELVA" in n: 
                                return 'Gasdel Norte+ Gas de la Selva'
                            
                            # 3. Líquidos (Residual y Diésel)
                            elif "RESIDUAL" in n or "DIESEL" in n or "D2" in n: 
                                return 'Residual+ Diesel D2'
                            
                            # 4. Biomasa y Otros Gases (Bagazo, Biogás, Nafta, Flexigas)
                            elif "NAFTA" in n or "FLEXIGAS" in n or "BAGAZO" in n or "BIOGAS" in n or "BIOMASA" in n: 
                                return 'biogas+Biomasa+Nafta+Flexigas'
                            
                            # 5. Renovables (Solar y Eólica)
                            elif "SOLAR" in n or "FOTOVOLTAICA" in n: 
                                return 'Solar'
                            elif "EOL" in n: 
                                return 'Eolica'
                            
                            # Fallback de seguridad
                            else: 
                                return 'Otros'
                        
                        # 3. Mapear las cabeceras leídas a las categorías estándar requeridas
                        categorias_dinamicas = [clasificar_recurso(c) for c in cabeceras_crudas]
                        
                        # 4. Extraer los datos de potencia (Fila 7 a 54, índice 6 a 53)
                        data_rec = df_raw_rec.iloc[6:54, 2:15].values
                        
                        # 5. Construir DataFrame con nombres dinámicos
                        df_rec = pd.DataFrame(data_rec, columns=categorias_dinamicas)
                        df_rec['FECHA_HORA'] = fechas_horas
                        
                        # Transformar (Melt) y limpiar datos
                        df_rec_m = df_rec.melt(id_vars=['FECHA_HORA'], var_name='AGRUPACION', value_name='DESPACHO_MW')
                        df_rec_m['DESPACHO_MW'] = pd.to_numeric(df_rec_m['DESPACHO_MW'], errors='coerce').fillna(0)
                        
                        # 6. Agrupar las sumas por la categoría dinámica calculada
                        df_recurso_melt = df_rec_m.groupby(['FECHA_HORA', 'AGRUPACION'], as_index=False)['DESPACHO_MW'].sum()
                    return df_melt, df_recurso_melt, None
        except Exception:
            continue
            
    return pd.DataFrame(), pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] No se halló data operacional válida."

def procesar_rango_fechas(start_date, end_date, progress_bar, status_text):
    fechas = pd.date_range(start_date, end_date)
    total_dias = len(fechas)
    lista_dfs, lista_dfs_rec, alertas = [], [], []
    
    for i, f in enumerate(fechas):
        status_text.markdown(f"**⏳ Sincronizando datos de Despacho (COES):** {f.strftime('%d/%m/%Y')} *(Día {i+1} de {total_dias})*")
        df_dia, df_rec_dia, error = extraer_datos_despacho(f)
        
        if not df_dia.empty: lista_dfs.append(df_dia)
        if not df_rec_dia.empty: lista_dfs_rec.append(df_rec_dia)
        if error: alertas.append(error)
            
        progress_bar.progress((i + 1) / total_dias)
            
    df_final = pd.concat(lista_dfs, ignore_index=True) if lista_dfs else pd.DataFrame()
    df_rec_final = pd.concat(lista_dfs_rec, ignore_index=True) if lista_dfs_rec else pd.DataFrame()
    return df_final, df_rec_final, alertas

# --- 3. INTERFAZ DE USUARIO ---
st.sidebar.header("Parámetros de Fiscalización")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (IEOD)", value=(datetime(2026, 2, 26), datetime(2026, 2, 27)))

if st.sidebar.button("Extraer Curvas de Despacho", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        start_date, end_date = rango_fechas
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        df_consolidado, df_rec_consolidado, alertas = procesar_rango_fechas(start_date, end_date, progress_bar, status_text)
        
        st.session_state['df_despacho'] = df_consolidado
        st.session_state['df_recurso'] = df_rec_consolidado
        st.session_state['alertas_despacho'] = alertas
            
        status_text.empty()
        progress_bar.empty()

# --- 4. VISUALIZACIÓN DE DATOS ---
if 'df_despacho' in st.session_state:
    df_datos = st.session_state['df_despacho']
    df_recurso = st.session_state.get('df_recurso', pd.DataFrame())
    alertas = st.session_state['alertas_despacho']
    
    if df_datos.empty:
        st.error("🚨 Extracción fallida o sin datos operacionales.")
        if alertas:
            with st.expander("Ver bitácora de errores del COES"):
                for a in alertas: st.write(a)
    else:
        if alertas:
            with st.expander("⚠️ Alertas de Integración (Fallas puntuales)"):
                for alerta in alertas: st.warning(alerta)
                    
        st.success("✅ Extracción y vectorización de despacho completada con éxito.")
        st.markdown("---")
        
        es_formato_anexo1 = (df_datos['EMPRESA'] == 'N/A').all()
        
        st.markdown("### 🔍 Filtros Operativos")
        
        if es_formato_anexo1:
            lista_centrales = sorted(df_datos['CENTRAL'].unique())
            filtro_cen = st.multiselect("⚡ Centrales:", options=lista_centrales, placeholder="Todas")
            df_filtrado = df_datos[df_datos['CENTRAL'].isin(filtro_cen)] if filtro_cen else df_datos
        else:
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                lista_empresas = sorted(df_datos['EMPRESA'].unique())
                filtro_emp = st.multiselect("🏢 Empresa:", options=lista_empresas, placeholder="Todas")
            df_f1 = df_datos[df_datos['EMPRESA'].isin(filtro_emp)] if filtro_emp else df_datos
            
            with col_f2:
                lista_tipos = sorted(df_f1['TIPO_CENTRAL'].unique())
                filtro_tipo = st.multiselect("🏭 Tipo Generación:", options=lista_tipos, placeholder="Todos")
            df_f2 = df_f1[df_f1['TIPO_CENTRAL'].isin(filtro_tipo)] if filtro_tipo else df_f1
            
            with col_f3:
                lista_zonas = sorted(df_f2['ZONA'].unique())
                filtro_zona = st.multiselect("🌍 Zona:", options=lista_zonas, placeholder="Todas")
            df_f3 = df_f2[df_f2['ZONA'].isin(filtro_zona)] if filtro_zona else df_f2
            
            with col_f4:
                lista_centrales = sorted(df_f3['CENTRAL'].unique())
                filtro_cen = st.multiselect("⚡ Centrales:", options=lista_centrales, placeholder="Todas")
            df_filtrado = df_f3[df_f3['CENTRAL'].isin(filtro_cen)] if filtro_cen else df_f3

        if df_filtrado.empty:
            st.warning("⚠️ No hay datos despachados para la selección de filtros.")
        else:
            # ==========================================
            # MOTOR GRÁFICO
            # ==========================================
            def crear_grafica_area(df_grafico, col_color, titulo, color_map=None):
                df_plot = df_grafico.copy()
                
                # 🚨 PASO CRÍTICO: Eliminar datos ocultos o columnas "TOTAL" encubiertas.
                # Al eliminar nulos en la categoría, evitamos que columnas residuales 
                # dupliquen la sumatoria del cálculo del techo del eje Y.
                df_plot = df_plot.dropna(subset=[col_color])
                
                df_plot['DESPACHO_MW'] = pd.to_numeric(df_plot['DESPACHO_MW'], errors='coerce').fillna(0)
                
                # 1. Cálculo del techo del sistema (Ahora 100% puro y real)
                df_sistema = df_plot.groupby('FECHA_HORA', as_index=False)['DESPACHO_MW'].sum()
                max_demanda_real = df_sistema['DESPACHO_MW'].max()
                
                # Limite exacto: Máxima Demanda real + 5% de holgura visual
                limite_superior_y = max_demanda_real * 1.05 if pd.notna(max_demanda_real) and max_demanda_real > 0 else 10000

                # 2. Creación de la gráfica base
                fig = px.area(
                    df_plot, 
                    x="FECHA_HORA", 
                    y="DESPACHO_MW", 
                    color=col_color, 
                    title=titulo,
                    labels={col_color: "Unidad Generadora" if col_color == "CENTRAL" else "Tecnología"},
                    color_discrete_map=color_map
                )
                
                fig.update_traces(hovertemplate="%{y:,.2f} MW")
                
                # 3. Curva invisible para el Tooltip Maestro
                fig.add_scatter(
                    x=df_sistema['FECHA_HORA'], 
                    y=df_sistema['DESPACHO_MW'],
                    mode='lines',
                    line=dict(width=0, color='rgba(0,0,0,0)'),
                    name='<b>⚡ TOTAL SISTEMA</b>',
                    hovertemplate='<b>🗓️ %{x|%d/%m/%Y %H:%M} ➡️ %{y:,.2f} MW</b>',
                    showlegend=False
                )
                
                # 4. Ajuste Forzado de Layout
                fig.update_layout(
                    hovermode="x unified",
                    xaxis=dict(
                        tickformat="%d/%m\n%H:%M", 
                        title="Fecha Operativa",
                        hoverformat="<b>🗓️ %d/%m/%Y %H:%M</b>"
                    ),
                    yaxis=dict(
                        title="Potencia Activa (MW)",
                        range=[0, limite_superior_y], # <-- Límite matemático estricto
                        autorange=False,
                        fixedrange=False
                    ),
                    height=650 if col_color == 'CENTRAL' else 500,
                    margin=dict(t=50, b=50, l=50, r=20)
                )
                
                return fig

            # ==========================================
            # GRÁFICA 1: ÁREA APILADA POR CENTRALES 
            # ==========================================
            st.markdown("---")
            st.markdown("### 📊 Despacho Detallado por Unidades de Generación")
            
            energia_total_cen = df_filtrado.groupby('CENTRAL')['DESPACHO_MW'].sum()
            centrales_activas = energia_total_cen[energia_total_cen > 0].index
            
            df_plot_cen = df_filtrado[df_filtrado['CENTRAL'].isin(centrales_activas)].copy()
            energia_ordenada_cen = energia_total_cen[centrales_activas].sort_values(ascending=False).index
            df_plot_cen['CENTRAL'] = pd.Categorical(df_plot_cen['CENTRAL'], categories=energia_ordenada_cen, ordered=True)
            df_plot_cen = df_plot_cen.sort_values(['FECHA_HORA', 'CENTRAL'])

            fig_cen = crear_grafica_area(df_plot_cen, 'CENTRAL', "Despacho Ejecutado de Potencia por Unidad")
            st.plotly_chart(fig_cen, use_container_width=True)

            # ==========================================
            # GRÁFICA 2: ÁREA APILADA POR TIPO DE GENERACIÓN (NUEVA FUENTE: TIPO_RECURSO)
            # ==========================================
            if not es_formato_anexo1 and not df_recurso.empty:
                st.markdown("### 📊 Despacho por Tipo de Generación (Fuente: TIPO_RECURSO)")
                
                orden_requerido = [
                    "biogas+Biomasa+Nafta+Flexigas",
                    "Solar",
                    "Eolica",
                    "Hidraulica",
                    "Gasdel Norte+ Gas de la Selva",
                    "Gas de Camisea",
                    "Residual+ Diesel D2"
                ]
                
                # Paleta de colores ajustada: Hidráulica ahora es celeste
                colores_tecnologia = {
                    "biogas+Biomasa+Nafta+Flexigas": "purple",
                    "Solar": "yellow",
                    "Eolica": "gray",
                    "Hidraulica": "skyblue",
                    "Gasdel Norte+ Gas de la Selva": "lightgreen",
                    "Gas de Camisea": "darkgreen",
                    "Residual+ Diesel D2": "red"
                }
                
                df_tipo = df_recurso.copy()
                df_tipo['AGRUPACION'] = pd.Categorical(df_tipo['AGRUPACION'], categories=orden_requerido, ordered=True)
                df_tipo = df_tipo.sort_values(['FECHA_HORA', 'AGRUPACION'])

                fig_tipo = crear_grafica_area(df_tipo, 'AGRUPACION', "Curva de Carga Apilada por Tecnología de Despacho", color_map=colores_tecnologia)
                st.plotly_chart(fig_tipo, use_container_width=True)
            
            # ==========================================
            # TRAZABILIDAD MATRICIAL (PIVOT)
            # ==========================================
            st.markdown("---")
            st.markdown("### 🗄️ Trazabilidad de Potencia (Data Cruda - Vista Matricial)")
            
            df_pivot = df_plot_cen.copy()
            df_pivot['CENTRAL'] = df_pivot['CENTRAL'].astype(str)
            df_pivot['FECHA'] = df_pivot['FECHA_HORA'].dt.strftime('%d/%m/%Y')
            df_pivot['HORA'] = df_pivot['FECHA_HORA'].dt.strftime('%H:%M')
            
            if es_formato_anexo1:
                jerarquia_columnas = ['CENTRAL']
            else:
                jerarquia_columnas = ['ZONA', 'TIPO_CENTRAL', 'EMPRESA', 'CENTRAL']
            
            df_matricial = df_pivot.pivot_table(
                index=['FECHA', 'HORA'],
                columns=jerarquia_columnas,
                values='DESPACHO_MW',
                aggfunc='sum'
            )
            
            df_matricial = df_matricial.round(2)
            st.dataframe(df_matricial, use_container_width=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_matricial.to_excel(writer, sheet_name='Despacho_Crudo')
                
            st.download_button(
                label="📥 Descargar Vista Matricial (Excel)",
                data=buffer.getvalue(),
                file_name=f"matriz_despacho_coes_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="secondary"
            )