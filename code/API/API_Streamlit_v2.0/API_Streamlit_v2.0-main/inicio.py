# Importar las bibliotecas necesarias
import streamlit as st
import os
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
#import db_dtypes
from PIL import Image
import time
import streamlit_extras
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_option_menu import option_menu
from streamlit import *
#import plost
#import plotly.express as px
#import seaborn as sns
import pydeck as pdk
import numpy as np
##### Librerias Francisco - Modelo
from google.cloud import bigquery
from google.oauth2 import service_account
from ETLEnvironment_class import ETLEnvironment
import googlemaps
from statsmodels.tsa.arima.model import ARIMA
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static
import math
from nosotros import nosotros
from historia import historia
from comunidad import comunidad
from recomendaciones import recomendaciones
from modelo import main2
from cliente import cliente


st.set_page_config(page_title="Inicio", page_icon="star", layout="wide", initial_sidebar_state="collapsed")





# Crear una función para ejecutar las consultas en BigQuery y obtener un DataFrame
def run_query(query):
    path_root = ETLEnvironment().root_project_path
    json_credentials = "project-sismos-2a770c4ff889.json"
    first_scope = "https://www.googleapis.com/auth/cloud-platform"
    credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

   
selected = option_menu(
            menu_title=None,
            options=["Nosotros", "Historia", "Comunidad", "Recomendaciones","Modelo", "Clientes"],
            icons=["book" ,"list-task","star","star","star","star"],
            menu_icon="cast",
            default_index=0,
            orientation="horizontal",
    styles={
        "container": {"padding": "0!important"}})

    
if selected == "Nosotros":
    nosotros()
if selected == "Historia":
    historia()
if selected == "Comunidad":
    comunidad()
if selected == "Recomendaciones":
    recomendaciones()
if selected == "Modelo":
    main2()
if selected == "Clientes":
    cliente()