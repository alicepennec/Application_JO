from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv

input_data = "/opt/airflow/dags/fact_resultats_epreuves.csv"

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def create_database():
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )
    conn.autocommit=True
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE sportdb")
    cursor.close()
    conn.close()

def extract_data():
    df = pd.read_csv(input_data, sep = ',')
    print(f"Données extraites : {df.shape[0]} lignes")
    return df.to_json()

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task')
    df = pd.read_json(data)

    df_transformed = df.drop(
        columns=[
            'id_resultat_source',
            'id_personne',
            'id_equipe',
            'id_pays',
            'id_evenement',
            'evenement_en',
            'id_edition',
            'id_competition_sport',
            'competition_en',
            'id_type_competition',
            'id_ville_edition',
            'edition_ville_en',
            'id_nation_edition_base_resultats',
            'id_sport',
            'sport_en',
            'id_discipline_administrative',
            'id_specialite',
            'id_epreuve',
            'id_federation',
            'federation_nom_court'
        ]
    ).drop_duplicates()
    return df_transformed.to_json()

def load_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_task')
    df = pd.read_json(data)
# Connexion via SQLAlchemy
    engine = create_engine("postgresql+psycopg2://user:pass@postgres:5432/sportdb")

# Création de la table
    with engine.begin() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS sports (
            id_resultat INT PRIMARY KEY,
            source TEXT,
            athlete_nom TEXT,
            athlete_prenom TEXT,
            equipe_en TEXT,
            pays_en_base_resultats TEXT,
            classement_epreuve FLOAT,
            performance_finale_texte TEXT,
            performance_finale FLOAT,
            evenement TEXT,
            categorie_age TEXT,
            competition_en TEXT,
            type_competition TEXT,
            edition_saison INT,
            date_debut_edition TIMESTAMP,
            date_fin_edition TIMESTAMP,
            edition_ville_en TEXT,
            edition_nation_en TEXT,
            sport TEXT,
            discipline_administrative TEXT,
            specialite TEXT,
            epreuve TEXT,
            epreuve_genre TEXT,
            epreuve_type TEXT,
            est_epreuve_individuelle INT,
            est_epreuve_olympique INT,
            est_epreuve_ete INT,
            est_epreuve_handi INT,
            epreuve_sens_resultat INT,
            federation TEXT,
            dt_creation TIMESTAMP,
            dt_modification TIMESTAMP
        );
        """)

    # Insertion avec Pandas (ignore si déjà présent via index)
    df.to_sql('sports', conn, if_exists='append', index=False)

# Définiton du dag
dag = DAG(
    'ingest_sports_data',
    schedule_interval='@hourly',
    start_date= datetime(2024, 1, 1),
    retries= 1,
    retry_delay= timedelta(minutes=5),
    catchup=False
)

# Ordre d'exécution des tâches
extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
trasform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, provide_context=True, dag=dag)
load_task = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True, dag=dag)
    
