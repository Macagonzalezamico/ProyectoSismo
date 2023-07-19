# Importar las bibliotecas necesarias
import uvicorn
import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
#import db_dtypes
from PIL import Image
import time
import streamlit_extras
#from streamlit_extras.switch_page_button import switch_page
#from streamlit_extras.metric_cards import style_metric_cards
from streamlit_option_menu import option_menu
from streamlit import *
#import plost
#import plotly.express as px
#import seaborn as sns
import pydeck as pdk
#import numpy as np
##### Librerias Francisco - Modelo
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from ETLEnvironment_class import ETLEnvironment
import pandas as pd
import googlemaps
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static
import math
import os

def nosotros():

    st.title("`Quienes Somos`")
    st.subheader("Somos una consultora con 3 años de experiencia en el mundo de los datos. Somos data-driven, todas nuestras acciones estan basadas y fundamentadas en datos")

    st.subheader("Contamos con un equipo multidisciplinario, donde cada uno posee grandes habilidades tecnicas y personales")

    st.title("`Mision`")
    st.subheader("Nuestra mision es ayudar a las organizaciones a aprovechar al máximo el poder de sus datos. Nos comprometemos a brindar soluciones innovadoras y estratégicas que impulsen la toma de decisiones basada en datos, fomenten la eficiencia operativa y generen un impacto positivo en el rendimiento y el crecimiento de nuestros clientes.")

    st.title("`Vision`")
    st.subheader("La visión como consultora de datos es convertirnos en líderes reconocidos a nivel mundial en el campo de la ciencia de datos y análisis de datos. Nos esforzamos por ser la consultora de referencia para las organizaciones que buscan maximizar el valor de sus datos y utilizarlos como un activo estratégico para la toma de decisiones.",)

    st.title("`Nuestro Equipo`")
    col1, col2, col3, col4 = st.columns(4)
    with col1:  
        st.subheader("`Data Analyst`: Macarena Gonzalez")
    with col2:
        st.subheader("`Data Scientist`: Francisco Krapovickas")
    with col3:
        st.subheader("`Data Engineer`: Fernando Embrioni")
    with col4:
        st.subheader("`Data Engineer`: Michel Villot")

