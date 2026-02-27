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
    
    for url, tipo_anexo in urls:
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                archivo_excel = io.BytesIO(res.content)
                xls = pd.ExcelFile(archivo_excel, engine='openpyxl')
                hojas_limpias = {h.strip().upper(): h for h in xls.sheet_names}
                
                if "DESPACHO_EJECUTADO" in hojas_limpias:
                    nombre_real = hojas_limpias["DESPACHO_EJECUTADO"]
                    df_raw = pd.read_excel(xls, sheet_name=nombre_real, header=None)
                    
                    # Discriminación de layout por tipo de archivo y extracción de metadata
                    if tipo_anexo == "AnexoA":
                        zonas_raw = df_raw.iloc[6, 2:].values
                        tipos_raw = df_raw.iloc[7, 2:].values
                        empresas_raw = df_raw.iloc[8, 2:].values
                        plantas_raw = df_raw.iloc[9, 2:].values
                        data_raw = df_raw.iloc[10:58, 2:].values
                    else: # Anexo1 (Legacy)
                        plantas_raw = df_raw.iloc[5, 1:].values
                        data_raw = df_raw.iloc[6:54, 1:].values
                        # Relleno para metadatos inexistentes en el layout antiguo
                        zonas_raw = ["N/A"] * len(plantas_raw)
                        tipos_raw = ["N/A"] * len(plantas_raw)
                        empresas_raw = ["N/A"] * len(plantas_raw)
                    
                    idx_validos = []
                    nombres_plantas = []
                    dict_metadatos = {}
                    
                    # Filtro estricto de columnas (limpieza de vacíos/NaN y exclusión de totales 'MW')
                    for i, p in enumerate(plantas_raw):
                        if pd.notna(p):
                            nombre = str(p).strip().upper()
                            if nombre != '' and 'MW' not in nombre:
                                idx_validos.append(i)
                                nombres_plantas.append(nombre)
                                
                                # Almacenar metadata asociada a la central
                                dict_metadatos[nombre] = {
                                    'ZONA': str(zonas_raw[i]).strip().upper() if pd.notna(zonas_raw[i]) else "N/A",
                                    'TIPO_CENTRAL': str(tipos_raw[i]).strip().upper() if pd.notna(tipos_raw[i]) else "N/A",
                                    'EMPRESA': str(empresas_raw[i]).strip().upper() if pd.notna(empresas_raw[i]) else "N/A"
                                }
                    
                    datos_limpios = data_raw[:, idx_validos]
                    
                    # Consolidación diaria
                    df_dia = pd.DataFrame(datos_limpios, columns=nombres_plantas)
                    fechas_horas = [fecha + timedelta(minutes=30 * (i + 1)) for i in range(48)]
                    df_dia['FECHA_HORA'] = fechas_horas
                    
                    # Transformación de matriz a tabla plana
                    df_melt = df_dia.melt(id_vars=['FECHA_HORA'], var_name='CENTRAL', value_name='DESPACHO_MW')
                    
                    # Limpieza numérica de despacho
                    df_melt['DESPACHO_MW'] = pd.to_numeric(df_melt['DESPACHO_MW'], errors='coerce').fillna(0)
                    
                    # Mapeo de metadata a la tabla plana
                    df_melt['ZONA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['ZONA'])
                    df_melt['TIPO_CENTRAL'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['TIPO_CENTRAL'])
                    df_melt['EMPRESA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['EMPRESA'])
                    
                    return df_melt, None
        except Exception as e:
            continue
            
    return pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] No se halló el archivo o la hoja 'DESPACHO_EJECUTADO'."

def procesar_rango_fechas(start_date, end_date, progress_bar, status_text):
    fechas = pd.date_range(start_date, end_date)
    total_dias = len(fechas)
    lista_dfs = []
    alertas = []
    
    for i, f in enumerate(fechas):
        status_text.markdown(f"**⏳ Sincronizando datos de Despacho (COES):** {f.strftime('%d/%m/%Y')} *(Día {i+1} de {total_dias})*")
        df_dia, error = extraer_datos_despacho(f)
        
        if not df_dia.empty:
            lista_dfs.append(df_dia)
        if error:
            alertas.append(error)
            
        progress_bar.progress((i + 1) / total_dias)
            
    if lista_dfs:
        return pd.concat(lista_dfs, ignore_index=True), alertas
    return pd.DataFrame(), alertas

# --- 3. INTERFAZ DE USUARIO ---
st.sidebar.header("Parámetros de Fiscalización")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (IEOD)", value=(datetime(2026, 2, 26), datetime(2026, 2, 27)))

if st.sidebar.button("Extraer Curvas de Despacho", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        start_date, end_date = rango_fechas
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        df_consolidado, alertas = procesar_rango_fechas(start_date, end_date, progress_bar, status_text)
        
        st.session_state['df_despacho'] = df_consolidado
        st.session_state['alertas_despacho'] = alertas
            
        status_text.empty()
        progress_bar.empty()

# --- 4. VISUALIZACIÓN DE DATOS ---
if 'df_despacho' in st.session_state:
    df_datos = st.session_state['df_despacho']
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
        
        # Validación de metadatos (Detectar si todo es formato Anexo1 Legacy sin metadata)
        es_formato_anexo1 = (df_datos['EMPRESA'] == 'N/A').all()
        
        # ==========================================
        # FILTROS DINÁMICOS
        # ==========================================
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
            def anotar_maximo(figura, df_grafico):
                df_sistema = df_grafico.groupby('FECHA_HORA', as_index=False)['DESPACHO_MW'].sum()
                idx_max = df_sistema['DESPACHO_MW'].idxmax()
                pico_mw = df_sistema.loc[idx_max, 'DESPACHO_MW']
                pico_hora = df_sistema.loc[idx_max, 'FECHA_HORA']
                
                figura.add_annotation(
                    x=pico_hora, y=pico_mw,
                    text=f"<b>Pico Máximo: {pico_mw:,.2f} MW</b><br>{pico_hora.strftime('%d/%m %H:%M')}",
                    showarrow=True, arrowhead=2, arrowsize=1.5, arrowwidth=2, arrowcolor="#e74c3c",
                    ax=0, ay=-50, font=dict(size=12, color="#c0392b"),
                    bgcolor="rgba(255,255,255,0.8)", bordercolor="#c0392b", borderwidth=1, borderpad=4
                )
                return figura

            # ==========================================
            # GRÁFICA 1: ÁREA APILADA POR CENTRALES 
            # ==========================================
            st.markdown("---")
            st.markdown("### 📊 Despacho Detallado por Unidades de Generación")
            
            energia_total = df_filtrado.groupby('CENTRAL')['DESPACHO_MW'].sum().sort_values(ascending=False).index
            df_filtrado['CENTRAL'] = pd.Categorical(df_filtrado['CENTRAL'], categories=energia_total, ordered=True)
            df_plot = df_filtrado.sort_values(['FECHA_HORA', 'CENTRAL'])

            fig_cen = px.area(
                df_plot, x="FECHA_HORA", y="DESPACHO_MW", color="CENTRAL",
                title="Despacho Ejecutado de Potencia por Unidad",
                labels={"DESPACHO_MW": "Potencia Activa (MW)", "FECHA_HORA": "Fecha Operativa", "CENTRAL": "Central Térmica / Hidro"}
            )
            fig_cen = anotar_maximo(fig_cen, df_plot)
            fig_cen.update_layout(hovermode="x unified", height=650, xaxis=dict(tickformat="%d/%m\n%H:%M", tickangle=0))
            st.plotly_chart(fig_cen, use_container_width=True)

            # ==========================================
            # GRÁFICA 2: ÁREA APILADA POR TIPO DE GENERACIÓN 
            # ==========================================
            if not es_formato_anexo1:
                st.markdown("### 📊 Despacho por Tipo de Generación")
                
                df_tipo = df_filtrado.groupby(['FECHA_HORA', 'TIPO_CENTRAL'], as_index=False)['DESPACHO_MW'].sum()
                energia_tipo = df_tipo.groupby('TIPO_CENTRAL')['DESPACHO_MW'].sum().sort_values(ascending=False).index
                df_tipo['TIPO_CENTRAL'] = pd.Categorical(df_tipo['TIPO_CENTRAL'], categories=energia_tipo, ordered=True)
                df_tipo = df_tipo.sort_values(['FECHA_HORA', 'TIPO_CENTRAL'])

                fig_tipo = px.area(
                    df_tipo, x="FECHA_HORA", y="DESPACHO_MW", color="TIPO_CENTRAL",
                    title="Curva de Carga Apilada por Tecnología",
                    labels={"DESPACHO_MW": "Potencia Activa (MW)", "FECHA_HORA": "Fecha Operativa", "TIPO_CENTRAL": "Tecnología"}
                )
                fig_tipo = anotar_maximo(fig_tipo, df_tipo)
                fig_tipo.update_layout(hovermode="x unified", height=500, xaxis=dict(tickformat="%d/%m\n%H:%M", tickangle=0))
                st.plotly_chart(fig_tipo, use_container_width=True)
            
            # ==========================================
            # TRAZABILIDAD MATRICIAL (PIVOT)
            # ==========================================
            st.markdown("---")
            st.markdown("### 🗄️ Trazabilidad de Potencia (Data Cruda - Vista Matricial)")
            
            df_pivot = df_plot.copy()
            # Retirar tipo Categorical para evitar que la tabla Pivot cree columnas vacías por categorías no seleccionadas
            df_pivot['CENTRAL'] = df_pivot['CENTRAL'].astype(str)
            df_pivot['FECHA'] = df_pivot['FECHA_HORA'].dt.strftime('%d/%m/%Y')
            df_pivot['HORA'] = df_pivot['FECHA_HORA'].dt.strftime('%H:%M')
            
            # Configuramos la estructura de los encabezados según la disponibilidad de metadata
            if es_formato_anexo1:
                jerarquia_columnas = ['CENTRAL']
            else:
                jerarquia_columnas = ['ZONA', 'TIPO_CENTRAL', 'EMPRESA', 'CENTRAL']
            
            # Transformación Pivot Table
            df_matricial = df_pivot.pivot_table(
                index=['FECHA', 'HORA'],
                columns=jerarquia_columnas,
                values='DESPACHO_MW',
                aggfunc='sum'
            )
            
            # Formateo numérico para visualización limpia
            df_matricial = df_matricial.round(2)
            
            # Mostrar la tabla cruzada con MultiIndex (Streamlit la renderizará como en la imagen)
            st.dataframe(df_matricial, use_container_width=True)
            
            # --- DESCARGA A EXCEL CON CELDAS FUSIONADAS ---
            st.markdown("<br>", unsafe_allow_html=True)
            buffer = io.BytesIO()
            # Usar openpyxl preserva la estructura de MultiIndex de Pandas con celdas fusionadas
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_matricial.to_excel(writer, sheet_name='Despacho_Crudo')
                
            st.download_button(
                label="📥 Descargar Vista Matricial (Excel)",
                data=buffer.getvalue(),
                file_name=f"matriz_despacho_coes_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="secondary"
            )