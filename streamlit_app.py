import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

# --- Configuración de la página ---
st.set_page_config(
    page_title="Dashboard de Clientes",
    page_icon="📊",
    layout="wide"
)

# --- Conexión a Snowflake (con caché para mejor rendimiento) ---
@st.cache_resource
def get_snowflake_session():
    """Crea y cachea la sesión de Snowflake"""
    try:
        return Session.builder.configs(st.secrets["snowflake"]).create()
    except Exception as e:
        st.error(f"Error conectando a Snowflake: {e}")
        st.stop()

session = get_snowflake_session()

# --- Título y descripción ---
st.title("📊 Dashboard Interactivo de Clientes")
st.write("Esta app muestra la cantidad de clientes por segmento de mercado.")

# --- Filtro por segmento de mercado ---
segmentos_disponibles = ["HOUSEHOLD", "BUILDING", "FURNITURE", "AUTOMOBILE", "MACHINERY"]

segmento = st.selectbox(
    "🔍 Filtra por segmento de mercado:",
    ["Todos"] + segmentos_disponibles
)

# --- Función para obtener datos con caché ---
@st.cache_data(ttl=600)  # Cache por 10 minutos
def get_customer_data(_session, segmento_filtro):
    """Obtiene datos de clientes desde Snowflake"""
    try:
        if segmento_filtro == "Todos":
            query = """
                SELECT C_MKTSEGMENT, COUNT(*) AS CANTIDAD
                FROM CLIENTES
                GROUP BY C_MKTSEGMENT
                ORDER BY CANTIDAD DESC
            """
        else:
            # Nota: En producción, usa binding parameters si está disponible
            query = f"""
                SELECT C_MKTSEGMENT, COUNT(*) AS CANTIDAD
                FROM CLIENTES
                WHERE C_MKTSEGMENT = '{segmento_filtro}'
                GROUP BY C_MKTSEGMENT
                ORDER BY CANTIDAD DESC
            """
        
        df = _session.sql(query).to_pandas()
        df.columns = [col.lower() for col in df.columns]
        return df
    
    except Exception as e:
        st.error(f"Error ejecutando consulta: {e}")
        return pd.DataFrame()

# --- Obtener datos ---
with st.spinner("Cargando datos..."):
    df = get_customer_data(session, segmento)

# --- Verificar si hay datos ---
if df.empty:
    st.warning("No se encontraron datos para el segmento seleccionado.")
    st.stop()

# --- Layout con columnas ---
col1, col2 = st.columns([1, 3])

with col1:
    # KPI total de clientes
    total_clientes = int(df["cantidad"].sum())
    st.metric(
        label="👥 Total de Clientes", 
        value=f"{total_clientes:,}",
        help="Suma total de clientes en el segmento seleccionado"
    )
    
    # Mostrar número de segmentos
    num_segmentos = len(df)
    st.metric(
        label="📋 Segmentos",
        value=num_segmentos,
        help="Número de segmentos de mercado"
    )

with col2:
    # Gráfico de barras
    st.subheader("📈 Clientes por Segmento")
    st.bar_chart(df.set_index("c_mktsegment")["cantidad"])

# --- Tabla de datos detallados ---
st.subheader("📊 Datos Detallados")

# Formatear la tabla
df_display = df.copy()
df_display.columns = ["Segmento de Mercado", "Cantidad"]
df_display["Cantidad"] = df_display["Cantidad"].apply(lambda x: f"{x:,}")

st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True
)

# --- Información adicional ---
with st.expander("ℹ️ Información sobre los datos"):
    st.write("""
    - **Fuente:** Base de datos Snowflake
    - **Actualización:** Los datos se actualizan cada 10 minutos
    - **Segmentos disponibles:** HOUSEHOLD, BUILDING, FURNITURE, AUTOMOBILE, MACHINERY
    """)

# --- Footer ---
st.divider()
st.caption("Dashboard creado con Streamlit y Snowflake ❄️")
