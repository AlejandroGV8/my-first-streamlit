import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# --- Obtener la sesión activa de Snowflake ---
session = get_active_session()

# --- Título y descripción ---
st.title("Dashboard Interactivo de Clientes")
st.write("Esta app muestra la cantidad de clientes por segmento de mercado. Puedes filtrar por segmento.")

# --- Filtro por segmento (actualizado con tus datos) ---
segmento = st.selectbox(
    "Filtra por segmento de mercado:",
    ["Todos", "HOUSEHOLD", "BUILDING", "FURNITURE", "AUTOMOBILE", "MACHINERY"]
)

# --- Consulta SQL dinámica ---
if segmento == "Todos":
    query = """
    SELECT C_MKTSEGMENT, COUNT(*) AS cantidad
    FROM CLIENTES
    GROUP BY C_MKTSEGMENT
    ORDER BY cantidad DESC
    """
else:
    query = f"""
    SELECT C_MKTSEGMENT, COUNT(*) AS cantidad
    FROM CLIENTES
    WHERE C_MKTSEGMENT = '{segmento}'
    GROUP BY C_MKTSEGMENT
    ORDER BY cantidad DESC
    """

# --- Ejecutar la consulta y convertir a DataFrame ---
df = session.sql(query).to_pandas()

# --- Renombrar columnas a minúsculas ---
df.columns = [col.lower() for col in df.columns]

# --- Mostrar KPI total de clientes ---
st.metric("Total clientes", df["cantidad"].sum())

# --- Mostrar gráfico interactivo ---
st.subheader("Clientes por Segmento")
st.bar_chart(df.set_index("c_mktsegment"))

# --- Mostrar tabla con los datos debajo del gráfico ---
st.subheader("Datos detallados")
st.dataframe(df)
