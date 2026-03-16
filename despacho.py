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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9'
    }
    
    mapa_lomitas = {
        "PUNTA LOMITAS-I": "PUNTA LOMITAS", "PUNTA LOMITAS-II": "PUNTA LOMITAS",
        "P LOMITAS_EXP-BL1": "PUNTA LOMITAS EXPANSIÓN", "P LOMITAS_EXP-BL2": "PUNTA LOMITAS EXPANSIÓN",
        "PUNTA LOMITAS-BL1": "PUNTA LOMITAS", "PUNTA LOMITAS-BL2": "PUNTA LOMITAS",
        "PUN LOMITAS_EXP-BL1": "PUNTA LOMITAS EXPANSIÓN", "PUN LOMITAS_EXP-BL2": "PUNTA LOMITAS EXPANSIÓN"
    }
    
    errores_tecnicos = []
    
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

                    # --- EXTRACCIÓN HOJA 2: TIPO_RECURSO ---
                    df_recurso_melt = pd.DataFrame()
                    if tipo_anexo == "AnexoA" and "TIPO_RECURSO" in hojas_limpias:
                        hoja_rec = hojas_limpias["TIPO_RECURSO"]
                        df_raw_rec = pd.read_excel(xls, sheet_name=hoja_rec, header=None)
                        
                        row_headers = df_raw_rec.iloc[5, 2:].values
                        fin_col = 2
                        for val in row_headers:
                            if pd.isna(val) or str(val).strip() == "":
                                break
                            fin_col += 1
                            
                        cabeceras_crudas = df_raw_rec.iloc[5, 2:fin_col].values
                        
                        def normalizar_texto(texto):
                            if pd.isna(texto): return ""
                            t = str(texto).strip().upper()
                            t = t.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
                            t = " ".join(t.split())
                            return t

                        def clasificar_recurso(nombre):
                            n = normalizar_texto(nombre)
                            if "H. PASADA" in n or "H. REGULACION" in n or "HID" in n: return 'Hidraulica'
                            elif "CAMISEA" in n: return 'Gas de Camisea'
                            elif "MALACAS" in n or "AGUAYTIA" in n or "NORTE" in n or "SELVA" in n: return 'Gasdel Norte+ Gas de la Selva'
                            elif "RESIDUAL" in n or "DIESEL" in n or "D2" in n: return 'Residual+ Diesel D2'
                            elif "NAFTA" in n or "FLEXIGAS" in n or "BAGAZO" in n or "BIOGAS" in n or "BIOMASA" in n: return 'biogas+Biomasa+Nafta+Flexigas'
                            elif "SOLAR" in n or "FOTOVOLTAICA" in n: return 'Solar'
                            elif "EOL" in n: return 'Eolica'
                            else: return 'Otros'
                        
                        categorias_dinamicas = [clasificar_recurso(c) for c in cabeceras_crudas]
                        data_rec = df_raw_rec.iloc[6:54, 2:fin_col].values
                        df_rec = pd.DataFrame(data_rec, columns=categorias_dinamicas)
                        df_rec['FECHA_HORA'] = fechas_horas
                        
                        df_rec_m = df_rec.melt(id_vars=['FECHA_HORA'], var_name='AGRUPACION', value_name='DESPACHO_MW')
                        df_rec_m['DESPACHO_MW'] = pd.to_numeric(df_rec_m['DESPACHO_MW'], errors='coerce').fillna(0)
                        df_recurso_melt = df_rec_m.groupby(['FECHA_HORA', 'AGRUPACION'], as_index=False)['DESPACHO_MW'].sum()
                    
                    # --- EXTRACCIÓN HOJA 3: RESERVA FRÍA Y EFICIENTE ---
                    df_rf_melt = pd.DataFrame()
                    nombres_posibles_rf = [h for h in hojas_limpias.keys() if "RESERVA" in h and "FR" in h]
                    
                    if nombres_posibles_rf:
                        hoja_rf = hojas_limpias[nombres_posibles_rf[0]]
                        df_raw_rf = pd.read_excel(xls, sheet_name=hoja_rf, header=None)
                        
                        # Códigos de unidades a restar para obtener Reserva Eficiente
                        codigos_restriccion = [239, 263, 265, 240, 241, 242, 924, 926, 786, 787, 788, 789, 995, 996, 997, 758, 42667, 42688, 756, 156]
                        
                        col_rf = None
                        cols_restriccion = []
                        
                        # Búsqueda dinámica de la columna 7000 y de las restricciones
                        for idx_fila in range(3, 7): 
                            fila_vals = df_raw_rf.iloc[idx_fila].values
                            if 7000 in fila_vals:
                                fila_lista = list(fila_vals)
                                col_rf = fila_lista.index(7000)
                                # Buscar índices de las restricciones
                                for cod in codigos_restriccion:
                                    if cod in fila_lista:
                                        cols_restriccion.append(fila_lista.index(cod))
                                break
                        
                        if col_rf is not None:
                            # Reserva Fría Bruta
                            data_rf = df_raw_rf.iloc[6:54, col_rf].values
                            reserva_fria_series = pd.to_numeric(pd.Series(data_rf), errors='coerce').fillna(0)
                            
                            # Sumar curvas de las restricciones operativas
                            restriccion_total = pd.Series(np.zeros(48))
                            for col_idx in cols_restriccion:
                                data_res = df_raw_rf.iloc[6:54, col_idx].values
                                restriccion_total += pd.to_numeric(pd.Series(data_res), errors='coerce').fillna(0)
                                
                            # Calcular Reserva Eficiente
                            reserva_eficiente_series = reserva_fria_series - restriccion_total
                            reserva_eficiente_series = reserva_eficiente_series.clip(lower=0) # Evitar valores negativos por inconsistencias del COES
                            
                            df_rf_melt = pd.DataFrame({
                                'FECHA_HORA': fechas_horas,
                                'RESERVA_FRIA_MW': reserva_fria_series,
                                'RESERVA_EFICIENTE_MW': reserva_eficiente_series
                            })

                    return df_melt, df_recurso_melt, df_rf_melt, None
                else:
                    errores_tecnicos.append(f"{tipo_anexo}: Faltó hoja DESPACHO_EJECUTADO.")
            else:
                errores_tecnicos.append(f"{tipo_anexo}: HTTP {res.status_code}")
                
        except Exception as e:
            errores_tecnicos.append(f"Error nativo en {tipo_anexo}: {type(e).__name__} - {str(e)}")
            continue
            
    motivo_error = " | ".join(errores_tecnicos) if errores_tecnicos else "Falla desconocida."
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] ❌ Motivo: {motivo_error}"

def procesar_rango_fechas(start_date, end_date, progress_bar, status_text):
    fechas = pd.date_range(start_date, end_date)
    total_dias = len(fechas)
    lista_dfs, lista_dfs_rec, lista_dfs_rf, alertas = [], [], [], []
    
    for i, f in enumerate(fechas):
        status_text.markdown(f"**⏳ Sincronizando datos de Despacho (COES):** {f.strftime('%d/%m/%Y')} *(Día {i+1} de {total_dias})*")
        df_dia, df_rec_dia, df_rf_dia, error = extraer_datos_despacho(f)
        
        if not df_dia.empty: lista_dfs.append(df_dia)
        if not df_rec_dia.empty: lista_dfs_rec.append(df_rec_dia)
        if not df_rf_dia.empty: lista_dfs_rf.append(df_rf_dia)
        if error: alertas.append(error)
            
        progress_bar.progress((i + 1) / total_dias)
            
    df_final = pd.concat(lista_dfs, ignore_index=True) if lista_dfs else pd.DataFrame()
    df_rec_final = pd.concat(lista_dfs_rec, ignore_index=True) if lista_dfs_rec else pd.DataFrame()
    df_rf_final = pd.concat(lista_dfs_rf, ignore_index=True) if lista_dfs_rf else pd.DataFrame()
    return df_final, df_rec_final, df_rf_final, alertas

# --- 3. INTERFAZ DE USUARIO ---
st.sidebar.header("Parámetros de Fiscalización")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (IEOD)", value=(datetime(2026, 2, 26), datetime(2026, 2, 27)))

if st.sidebar.button("Extraer Curvas de Despacho", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        start_date, end_date = rango_fechas
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        df_consolidado, df_rec_consolidado, df_rf_consolidado, alertas = procesar_rango_fechas(start_date, end_date, progress_bar, status_text)
        
        st.session_state['df_despacho'] = df_consolidado
        st.session_state['df_recurso'] = df_rec_consolidado
        st.session_state['df_reserva_fria'] = df_rf_consolidado
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
                    
        st.success("✅ Extracción y vectorización completada con éxito.")
        st.markdown("---")
        
        # --- ESTRUCTURA DE PESTAÑAS ---
        tab_despacho, tab_reserva = st.tabs(["📊 Curvas de Despacho y Energía", "❄️ Reserva Fría y Eficiente"])
        
        with tab_despacho:
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
                # MOTOR GRÁFICO (CON ANCLAJE Y SIN BORDES)
                # ==========================================
                def crear_grafica_area(df_grafico, col_color, titulo, color_map=None):
                    df_plot = df_grafico.copy()
                    df_plot = df_plot.dropna(subset=[col_color])
                    df_plot['DESPACHO_MW'] = pd.to_numeric(df_plot['DESPACHO_MW'], errors='coerce').fillna(0)
                    
                    df_sistema = df_plot.groupby('FECHA_HORA', as_index=False)['DESPACHO_MW'].sum()
                    max_demanda_real = df_sistema['DESPACHO_MW'].max()
                    limite_superior_y = max_demanda_real * 1.05 if pd.notna(max_demanda_real) and max_demanda_real > 0 else 10000

                    fecha_min = df_plot['FECHA_HORA'].min()
                    fecha_max = df_plot['FECHA_HORA'].max()

                    fig = px.area(
                        df_plot, 
                        x="FECHA_HORA", 
                        y="DESPACHO_MW", 
                        color=col_color, 
                        title=titulo,
                        labels={col_color: "Unidad Generadora" if col_color == "CENTRAL" else "Tecnología"},
                        color_discrete_map=color_map
                    )
                    
                    fig.update_traces(hovertemplate="%{y:,.2f} MW", line=dict(width=0))
                    
                    fig.add_scatter(
                        x=df_sistema['FECHA_HORA'], 
                        y=df_sistema['DESPACHO_MW'],
                        mode='lines',
                        line=dict(width=0, color='rgba(0,0,0,0)'),
                        name='<b>⚡ TOTAL SISTEMA</b>',
                        hovertemplate='<b>🗓️ %{x|%d/%m/%Y %H:%M} ➡️ %{y:,.2f} MW</b>',
                        showlegend=False
                    )
                    
                    fig.update_layout(
                        hovermode="x unified",
                        xaxis=dict(
                            tickformat="%d/%m\n%H:%M", 
                            title="Fecha Operativa", 
                            hoverformat="<b>🗓️ %d/%m/%Y %H:%M</b>",
                            range=[fecha_min, fecha_max] 
                        ),
                        yaxis=dict(title="Potencia Activa (MW)", range=[0, limite_superior_y], autorange=False, fixedrange=False),
                        height=650 if col_color == 'CENTRAL' else 500,
                        margin=dict(t=50, b=50, l=50, r=20) 
                    )
                    return fig
                
                # ==========================================
                # SECCIÓN 1: ANÁLISIS POR CENTRALES
                # ==========================================
                st.markdown("---")
                st.markdown("### 📊 Análisis Detallado por Unidades de Generación")
                
                energia_total_cen = df_filtrado.groupby('CENTRAL')['DESPACHO_MW'].sum()
                centrales_activas = energia_total_cen[energia_total_cen > 0].index
                
                df_plot_cen = df_filtrado[df_filtrado['CENTRAL'].isin(centrales_activas)].copy()
                energia_ordenada_cen = energia_total_cen[centrales_activas].sort_values(ascending=False).index
                df_plot_cen['CENTRAL'] = pd.Categorical(df_plot_cen['CENTRAL'], categories=energia_ordenada_cen, ordered=True)
                df_plot_cen = df_plot_cen.sort_values(['FECHA_HORA', 'CENTRAL'])

                fig_cen = crear_grafica_area(df_plot_cen, 'CENTRAL', "1. Despacho Ejecutado de Potencia por Unidad (MW)")
                st.plotly_chart(fig_cen, use_container_width=True)

                df_plot_cen['FECHA_DIA'] = (df_plot_cen['FECHA_HORA'] - pd.Timedelta(minutes=1)).dt.date
                df_plot_cen['ENERGIA_MWH'] = df_plot_cen['DESPACHO_MW'] * 0.5 
                
                df_energia_cen = df_plot_cen.groupby(['FECHA_DIA', 'CENTRAL'], as_index=False)['ENERGIA_MWH'].sum()
                df_total_dia_cen = df_energia_cen.groupby('FECHA_DIA', as_index=False)['ENERGIA_MWH'].sum()
                limite_y_cen = df_total_dia_cen['ENERGIA_MWH'].max() * 1.15 
                
                dia_min_cen = df_energia_cen['FECHA_DIA'].min() - pd.Timedelta(hours=12)
                dia_max_cen = df_energia_cen['FECHA_DIA'].max() + pd.Timedelta(hours=12)

                fig_bar_cen = px.bar(
                    df_energia_cen,
                    x='FECHA_DIA',
                    y='ENERGIA_MWH',
                    color='CENTRAL',
                    title="2. Despacho de Energía Diaria por Unidad (MWh)",
                    labels={'FECHA_DIA': 'Día Operativo', 'ENERGIA_MWH': 'Energía (MWh)', 'CENTRAL': 'Unidad'},
                    category_orders={'CENTRAL': energia_ordenada_cen}
                )
                
                fig_bar_cen.add_scatter(
                    x=df_total_dia_cen['FECHA_DIA'],
                    y=df_total_dia_cen['ENERGIA_MWH'],
                    mode='text',
                    text=df_total_dia_cen['ENERGIA_MWH'].apply(lambda x: f"<b>{x:,.1f} MWh</b>"),
                    textposition='top center',
                    showlegend=False,
                    hoverinfo='skip' 
                )

                fig_bar_cen.update_layout(
                    barmode='stack', hovermode="x unified",
                    xaxis=dict(
                        tickformat="%d/%m/%Y", 
                        title="Día Operativo", 
                        tickmode="linear",      
                        dtick=86400000,
                        range=[dia_min_cen, dia_max_cen]
                    ),
                    yaxis=dict(title="Energía Activa (MWh)", range=[0, limite_y_cen]), 
                    margin=dict(t=50, b=50, l=50, r=20)
                )
                fig_bar_cen.update_traces(hovertemplate="%{y:,.2f} MWh")
                st.plotly_chart(fig_bar_cen, use_container_width=True)

                # ==========================================
                # SECCIÓN 2: ANÁLISIS POR TECNOLOGÍA
                # ==========================================
                if not es_formato_anexo1 and not df_recurso.empty:
                    st.markdown("---")
                    st.markdown("### 📊 Análisis por Tipo de Generación (Fuente: TIPO_RECURSO)")
                    
                    orden_requerido = [
                        "biogas+Biomasa+Nafta+Flexigas", "Solar", "Eolica", "Hidraulica",
                        "Gasdel Norte+ Gas de la Selva", "Gas de Camisea", "Residual+ Diesel D2"
                    ]
                    
                    colores_tecnologia = {
                        "biogas+Biomasa+Nafta+Flexigas": "purple", "Solar": "yellow", "Eolica": "gray",
                        "Hidraulica": "skyblue", "Gasdel Norte+ Gas de la Selva": "lightgreen",
                        "Gas de Camisea": "darkgreen", "Residual+ Diesel D2": "red"
                    }
                    
                    df_tipo = df_recurso.copy()
                    df_tipo['AGRUPACION'] = pd.Categorical(df_tipo['AGRUPACION'], categories=orden_requerido, ordered=True)
                    df_tipo = df_tipo.sort_values(['FECHA_HORA', 'AGRUPACION'])

                    fig_tipo = crear_grafica_area(df_tipo, 'AGRUPACION', "1. Curva de Carga Apilada por Tecnología de Despacho (MW)", color_map=colores_tecnologia)
                    st.plotly_chart(fig_tipo, use_container_width=True)

                    df_tipo['FECHA_DIA'] = (df_tipo['FECHA_HORA'] - pd.Timedelta(minutes=1)).dt.date
                    df_tipo['ENERGIA_MWH'] = df_tipo['DESPACHO_MW'] * 0.5 
                    
                    df_energia_tipo = df_tipo.groupby(['FECHA_DIA', 'AGRUPACION'], as_index=False)['ENERGIA_MWH'].sum()
                    df_total_dia_tipo = df_energia_tipo.groupby('FECHA_DIA', as_index=False)['ENERGIA_MWH'].sum()
                    limite_y_tipo = df_total_dia_tipo['ENERGIA_MWH'].max() * 1.15
                    
                    dia_min_tipo = df_energia_tipo['FECHA_DIA'].min() - pd.Timedelta(hours=12)
                    dia_max_tipo = df_energia_tipo['FECHA_DIA'].max() + pd.Timedelta(hours=12)

                    fig_bar_tipo = px.bar(
                        df_energia_tipo,
                        x='FECHA_DIA',
                        y='ENERGIA_MWH',
                        color='AGRUPACION',
                        title="2. Despacho de Energía Diaria por Tecnología (MWh)",
                        labels={'FECHA_DIA': 'Día Operativo', 'ENERGIA_MWH': 'Energía (MWh)', 'AGRUPACION': 'Tecnología'},
                        color_discrete_map=colores_tecnologia,
                        category_orders={'AGRUPACION': orden_requerido}
                    )
                    
                    fig_bar_tipo.add_scatter(
                        x=df_total_dia_tipo['FECHA_DIA'],
                        y=df_total_dia_tipo['ENERGIA_MWH'],
                        mode='text',
                        text=df_total_dia_tipo['ENERGIA_MWH'].apply(lambda x: f"<b>{x:,.1f} MWh</b>"),
                        textposition='top center',
                        showlegend=False,
                        hoverinfo='skip'
                    )

                    fig_bar_tipo.update_layout(
                        barmode='stack', hovermode="x unified",
                        xaxis=dict(
                            tickformat="%d/%m/%Y", 
                            title="Día Operativo",
                            tickmode="linear", 
                            dtick=86400000,
                            range=[dia_min_tipo, dia_max_tipo]
                        ),
                        yaxis=dict(title="Energía Activa (MWh)", range=[0, limite_y_tipo]),
                        margin=dict(t=50, b=50, l=50, r=20)
                    )
                    fig_bar_tipo.update_traces(hovertemplate="%{y:,.2f} MWh")
                    st.plotly_chart(fig_bar_tipo, use_container_width=True)
                
                # ==========================================
                # TRAZABILIDAD MATRICIAL
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

        # ==========================================
        # PESTAÑA 2: RESERVA FRÍA Y EFICIENTE
        # ==========================================
        with tab_reserva:
            st.markdown("### ❄️ Fiscalización de Reserva Operativa del SEIN")
            df_rf = st.session_state.get('df_reserva_fria', pd.DataFrame())
            
            if df_rf.empty:
                st.warning("⚠️ No se encontró información de Reserva Fría para las fechas seleccionadas o el formato del COES no contiene la hoja esperada.")
            else:
                fecha_min_rf = df_rf['FECHA_HORA'].min()
                fecha_max_rf = df_rf['FECHA_HORA'].max()
                
                # ------------------------------------------
                # 1. GRÁFICA: RESERVA FRÍA TOTAL
                # ------------------------------------------
                st.markdown("#### 1. Disponibilidad de Reserva Fría Total")
                limite_superior_rf = df_rf['RESERVA_FRIA_MW'].max() * 1.10

                fig_rf = px.area(
                    df_rf, 
                    x="FECHA_HORA", 
                    y="RESERVA_FRIA_MW", 
                    title="Curva de Reserva Fría Total (MW)",
                    color_discrete_sequence=["#00BFFF"] 
                )
                fig_rf.update_traces(hovertemplate="<b>%{y:,.2f} MW</b>", line=dict(width=0))
                fig_rf.update_layout(
                    hovermode="x unified",
                    xaxis=dict(tickformat="%d/%m\n%H:%M", title="Fecha Operativa", range=[fecha_min_rf, fecha_max_rf]),
                    yaxis=dict(title="Reserva Total (MW)", range=[0, limite_superior_rf]),
                    height=350,
                    margin=dict(t=30, b=40, l=50, r=20)
                )
                st.plotly_chart(fig_rf, use_container_width=True)
                
                # Estadísticas Reserva Fría
                col_rf1, col_rf2, col_rf3 = st.columns(3)
                col_rf1.metric("Promedio - Reserva Fría", f"{df_rf['RESERVA_FRIA_MW'].mean():.2f} MW")
                col_rf2.metric("Máxima - Reserva Fría", f"{df_rf['RESERVA_FRIA_MW'].max():.2f} MW")
                col_rf3.metric("Mínima - Reserva Fría", f"{df_rf['RESERVA_FRIA_MW'].min():.2f} MW")
                
                st.markdown("---")

                # ------------------------------------------
                # 2. GRÁFICA: RESERVA EFICIENTE
                # ------------------------------------------
                st.markdown("#### 2. Disponibilidad de Reserva Eficiente (Descontando Restricciones)")
                limite_superior_ef = df_rf['RESERVA_EFICIENTE_MW'].max() * 1.10

                fig_ef = px.area(
                    df_rf, 
                    x="FECHA_HORA", 
                    y="RESERVA_EFICIENTE_MW", 
                    title="Curva de Reserva Eficiente (MW)",
                    color_discrete_sequence=["#32CD32"] # Verde lima para diferenciar de la Fría
                )
                fig_ef.update_traces(hovertemplate="<b>%{y:,.2f} MW</b>", line=dict(width=0))
                fig_ef.update_layout(
                    hovermode="x unified",
                    xaxis=dict(tickformat="%d/%m\n%H:%M", title="Fecha Operativa", range=[fecha_min_rf, fecha_max_rf]),
                    yaxis=dict(title="Reserva Eficiente (MW)", range=[0, limite_superior_ef]),
                    height=350,
                    margin=dict(t=30, b=40, l=50, r=20)
                )
                st.plotly_chart(fig_ef, use_container_width=True)
                
                # Estadísticas Reserva Eficiente
                col_ef1, col_ef2, col_ef3 = st.columns(3)
                col_ef1.metric("Promedio - Reserva Eficiente", f"{df_rf['RESERVA_EFICIENTE_MW'].mean():.2f} MW")
                col_ef2.metric("Máxima - Reserva Eficiente", f"{df_rf['RESERVA_EFICIENTE_MW'].max():.2f} MW")
                col_ef3.metric("Mínima - Reserva Eficiente", f"{df_rf['RESERVA_EFICIENTE_MW'].min():.2f} MW")