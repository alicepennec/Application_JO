import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

# Connexion SQLAlchemy
engine = create_engine("postgresql+psycopg2://user:pass@postgres:5432/sportdb")

st.title("SQL Playground - Données Sportives")

query = st.text_area("Entrez votre requête SQL ici :")

if st.button("Exécuter"):
    try:
        df = pd.read_sql_query(query, engine)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Erreur : {e}")
