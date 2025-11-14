import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Connexion SQLAlchemy
engine = create_engine("postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


st.title("SQL Playground - Données Sportives")

query = st.text_area("Entrez votre requête SQL ici :")

if st.button("Exécuter"):
    try:
        df = pd.read_sql_query(query, engine)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Erreur : {e}")
