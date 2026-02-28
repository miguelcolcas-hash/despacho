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
    
    # Diccionario de homologación para Punta Lomitas (Ambos formatos)
    mapa_lomitas = {
        "PUNTA LOMITAS-I": "PUNTA LOMITAS",
        "PUNTA LOMITAS-II": "PUNTA LOMITAS",
        "P LOMITAS_EXP-BL1": "PUNTA LOMITAS EXPANSIÓN",
        "P LOMITAS_EXP-BL2": "PUNTA LOMITAS EXPANSIÓN",
        "PUNTA LOMITAS-BL1": "PUNTA LOMITAS",
        "PUNTA LOMITAS-BL2": "PUNTA LOMITAS",
        "PUN LOMITAS_EXP-BL1": "PUNTA LOMITAS EXPANSIÓN",
        "PUN LOMITAS_EXP-BL2": "PUNTA LOMITAS EXPANSIÓN"
    }
    
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
                    
                    idx_validos = []
                    nombres_plantas = []
                    dict_metadatos = {}
                    
                    for i, p in enumerate(plantas_raw):
                        if pd.notna(p):
                            nombre_base_crudo = str(p).strip().upper()
                            if nombre_base_crudo != '' and 'MW' not in nombre_base_crudo:
                                
                                # 1. Aplicar Homologación Eólica
                                nombre_base = mapa_lomitas.get(nombre_base_crudo, nombre_base_crudo)
                                tipo_original = str(tipos_raw[i]).strip().upper() if pd.notna(tipos_raw[i]) else "N/A"
                                
                                # Forzar tipo EOLICA para Punta Lomitas si es Anexo1 (N/A) para mantener la abreviatura (EOL)
                                if nombre_base in ["PUNTA LOMITAS", "PUNTA LOMITAS EXPANSIÓN"] and tipo_original == "N/A":
                                    tipo_original = "EOLICA"
                                
                                # 2. Asignar Abreviaturas
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
                                
                                # Guardar metadata (Se sobrescribirá para bloques consolidados, lo cual es correcto)
                                dict_metadatos[nombre_central] = {
                                    'ZONA': str(zonas_raw[i]).strip().upper() if pd.notna(zonas_raw[i]) else "N/A",
                                    'TIPO_CENTRAL': tipo_original,
                                    'EMPRESA': str(empresas_raw[i]).strip().upper() if pd.notna(empresas_raw[i]) else "N/A"
                                }
                    
                    datos_limpios = data_raw[:, idx_validos]
                    
                    df_dia = pd.DataFrame(datos_limpios, columns=nombres_plantas)
                    fechas_horas = [fecha + timedelta(minutes=30 * (i + 1)) for i in range(48)]
                    df_dia['FECHA_HORA'] = fechas_horas
                    
                    # Melt permite duplicados en columnas (ej. dos bloques P. Lomitas)
                    df_melt = df_dia.melt(id_vars=['FECHA_HORA'], var_name='CENTRAL', value_name='DESPACHO_MW')
                    df_melt['DESPACHO_MW'] = pd.to_numeric(df_melt['DESPACHO_MW'], errors='coerce').fillna(0)
                    
                    df_melt['ZONA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['ZONA'])
                    df_melt['TIPO_CENTRAL'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['TIPO_CENTRAL'])
                    df_melt['EMPRESA'] = df_melt['CENTRAL'].map(lambda x: dict_metadatos[x]['EMPRESA'])
                    
                    # 3. Consolidación de sumas agrupadas (Une definitivamente los bloques de P. Lomitas)
                    df_melt = df_melt.groupby(['FECHA_HORA', 'CENTRAL', 'ZONA', 'TIPO_CENTRAL', 'EMPRESA'], as_index=False)['DESPACHO_MW'].sum()
                    
                    return df_melt, None
        except Exception:
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
            # MOTOR GRÁFICO (NATIVO X UNIFIED - OPTIMIZADO)
            # ==========================================
            def crear_grafica_area(df_grafico, col_color, titulo):
                df_plot = df_grafico.copy()
                
                df_plot['DESPACHO_PLOT'] = df_plot['DESPACHO_MW'].replace(0, np.nan)
                
                df_sistema = df_plot.groupby('FECHA_HORA', as_index=False)['DESPACHO_MW'].sum()
                idx_max = df_sistema['DESPACHO_MW'].idxmax()
                pico_mw = df_sistema.loc[idx_max, 'DESPACHO_MW']
                pico_hora = df_sistema.loc[idx_max, 'FECHA_HORA']
                
                fig = px.area(
                    df_plot, x="FECHA_HORA", y="DESPACHO_PLOT", color=col_color, title=titulo,
                    labels={col_color: "Unidad Generadora" if col_color == "CENTRAL" else "Tecnología"}
                )
                
                fig.update_traces(hovertemplate="%{y:,.2f} MW")
                
                fig.add_scatter(
                    x=df_sistema['FECHA_HORA'], 
                    y=df_sistema['DESPACHO_MW'],
                    mode='lines',
                    line=dict(width=0, color='rgba(0,0,0,0)'),
                    name='<b>⚡ TOTAL SISTEMA</b>',
                    hovertemplate='<b>%{y:,.2f} MW</b>',
                    showlegend=False
                )
                
                fig.add_annotation(
                    x=pico_hora, y=pico_mw,
                    text=f"<b>Pico Máximo: {pico_mw:,.2f} MW</b><br>{pico_hora.strftime('%d/%m %H:%M')}",
                    showarrow=True, arrowhead=2, arrowsize=1.5, arrowwidth=2, arrowcolor="#e74c3c",
                    ax=0, ay=-50, font=dict(size=12, color="#c0392b"),
                    bgcolor="rgba(255,255,255,0.8)", bordercolor="#c0392b", borderwidth=1, borderpad=4
                )
                
                fig.update_layout(
                    hovermode="x unified",
                    xaxis=dict(
                        tickformat="%d/%m\n%H:%M", 
                        title="Fecha Operativa",
                        hoverformat="<b>🗓️ %d/%m/%Y %H:%M</b>"
                    ),
                    yaxis=dict(title="Potencia Activa (MW)"),
                    height=650 if col_color == 'CENTRAL' else 500
                )
                return fig

            # ==========================================
            # GRÁFICA 1: ÁREA APILADA POR CENTRALES 
            # ==========================================
            st.markdown("---")
            st.markdown("### 📊 Despacho Detallado por Unidades de Generación")
            
            # FILTRO PARA LEYENDAS: Retener solo las centrales que despacharon > 0 MW en el total del rango filtrado
            energia_total_cen = df_filtrado.groupby('CENTRAL')['DESPACHO_MW'].sum()
            centrales_activas = energia_total_cen[energia_total_cen > 0].index
            
            df_plot_cen = df_filtrado[df_filtrado['CENTRAL'].isin(centrales_activas)].copy()
            
            energia_ordenada_cen = energia_total_cen[centrales_activas].sort_values(ascending=False).index
            df_plot_cen['CENTRAL'] = pd.Categorical(df_plot_cen['CENTRAL'], categories=energia_ordenada_cen, ordered=True)
            df_plot_cen = df_plot_cen.sort_values(['FECHA_HORA', 'CENTRAL'])

            fig_cen = crear_grafica_area(df_plot_cen, 'CENTRAL', "Despacho Ejecutado de Potencia por Unidad")
            st.plotly_chart(fig_cen, use_container_width=True)

            # ==========================================
            # GRÁFICA 2: ÁREA APILADA POR TIPO DE GENERACIÓN 
            # ==========================================
            if not es_formato_anexo1:
                st.markdown("### 📊 Despacho por Tipo de Generación")
                
                # FILTRO PARA LEYENDAS (TIPO)
                energia_total_tipo = df_filtrado.groupby('TIPO_CENTRAL')['DESPACHO_MW'].sum()
                tipos_activos = energia_total_tipo[energia_total_tipo > 0].index
                
                df_plot_tipo = df_filtrado[df_filtrado['TIPO_CENTRAL'].isin(tipos_activos)].copy()
                
                df_tipo = df_plot_tipo.groupby(['FECHA_HORA', 'TIPO_CENTRAL'], as_index=False)['DESPACHO_MW'].sum()
                energia_ordenada_tipo = energia_total_tipo[tipos_activos].sort_values(ascending=False).index
                df_tipo['TIPO_CENTRAL'] = pd.Categorical(df_tipo['TIPO_CENTRAL'], categories=energia_ordenada_tipo, ordered=True)
                df_tipo = df_tipo.sort_values(['FECHA_HORA', 'TIPO_CENTRAL'])

                fig_tipo = crear_grafica_area(df_tipo, 'TIPO_CENTRAL', "Curva de Carga Apilada por Tecnología")
                st.plotly_chart(fig_tipo, use_container_width=True)
            
            # ==========================================
            # TRAZABILIDAD MATRICIAL (PIVOT)
            # ==========================================
            st.markdown("---")
            st.markdown("### 🗄️ Trazabilidad de Potencia (Data Cruda - Vista Matricial)")
            
            # Usamos df_plot_cen para que la tabla matricial también excluya a las unidades apagadas 
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