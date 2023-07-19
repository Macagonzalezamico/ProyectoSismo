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


def run_query(query):
    path_root = ETLEnvironment().root_project_path
    json_credentials = "project-sismos-2a770c4ff889.json"
    first_scope = "https://www.googleapis.com/auth/cloud-platform"
    credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

def cliente():

    
    clave = "123456"
    with st.expander("Login"):
        col1, col2, col3 = st.columns(3)
        with col1:
            usuario = st.text_input("Ingresa el nombre de tu empresa")
        with col3:
            pass
        with col2:
            pregunta = st.text_input("Ingresa tu clave", help="Clave generica 123456", type="password")
            if clave == pregunta:
                with st.spinner("Validando usuario..."):
                    time.sleep(3)

    if pregunta == clave:
        st.title(f"`Reporte de Ganancias y Perdidas - KPIs de la empresa {usuario.upper()}`")
        col1, col2, col3 = st.columns(3)
        with col1:
            cliente = st.selectbox("Define que tipo de cliente eres", options=["Banco", "Seguro"])
        with col2:
            anio_actual = st.selectbox("Año a Consultar", options=[2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022], help="Siempre debe ser menor al Año Final")
        with col3:
            anio_previo = st.selectbox("Año anterior", options=[2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022])
        #Año actual


        query_actual = f"SELECT * FROM `sismos_db.damages_complemento` WHERE cliente like '{cliente}%' and anio = {anio_actual};"
        df_actual = run_query(query_actual)

            #Año previo
        query_previo = f"SELECT * FROM `sismos_db.damages_complemento` WHERE cliente like '{cliente}%' and anio = {anio_previo};"
        df_previo = run_query(query_previo)

        #KPIs
        """     
        1) Disminuir los gastos en siniestros debido a sismos en un 2% con relación al año anterior.
        2) Incrementar la tasa de asegurados en un 15% respecto al año anterior.
        3) Incrementar la tasa de Ingresos en un 2% respecto al año anterior.
        4) Incrementar el promedio de ingresos por asegurado en un 2% respecto al año anterior.
                    
                """

        #KPI1
        comparacion = df_previo["total_gasto"].sum()
        objetivo = comparacion*0.98
        actual = df_actual["total_gasto"].sum()

        #redondeamos
        comparacion = int(comparacion)
        objetivo = int(objetivo)
        actual = int(actual)


        #KPI 2
        comparacion_2 = df_previo["total_asegurados"].sum()
        objetivo_2 = comparacion_2*1.15
        actual_2 = df_actual["total_asegurados"].sum()

        #redondeamos
        comparacion_2 = int(comparacion)
        objetivo_2 = int(objetivo_2)
        actual_2 = int(actual_2)



        #KPI 3
        comparacion_3 = df_previo["total_ingresos"].sum()
        objetivo_3 = comparacion_3*1.02
        actual_3 = df_actual["total_ingresos"].sum()

        #redondeamos
        comparacion_3 = int(comparacion_3)
        objetivo_3 = int(objetivo_3)
        actual_3 = int(actual_3)
        

        #KPI 4
        comparacion_4 = df_previo["total_ingresos"].sum() / df_previo["total_asegurados"].sum()
        objetivo_4 = comparacion_4*1.02
        actual_4 = df_actual["total_ingresos"].sum() / df_actual["total_asegurados"].sum()

        #redondeamos
        comparacion_4 = int(comparacion_4)
        objetivo_4 = int(objetivo_4)
        actual_4 = int(actual_4)





        col1, col2 = st.columns(2)
        #KPI 1
        with col1:
            expand = st.expander("KPI - Disminuir los gastos en siniestros debido a sismos en un 2%.")
            with expand:
                if actual > objetivo:
                        st.write("`KPI - Disminuir los gastos en siniestros debido a sismos en un 2%.`")
                        st.metric(f"Gasto Año {anio_actual}", f"{actual}", delta=f"{objetivo}")
                        st.write(f"El objetivo para el año {anio_actual} era de ${objetivo}")
                        st.write(f"El gasto total del año {anio_actual} fue de ${actual} lo que representa un `{((actual/objetivo)-1)*1:.2%}` por encima de nuestra meta")
                elif actual < objetivo:
                        st.write("`KPI - Disminuir los gastos en siniestros debido a sismos en un 2%.`")
                        st.metric(f"Gasto Año {anio_actual}", f"{actual}", delta=f"{objetivo}")
                        st.write(f"El objetivo para el año {anio_actual} era de ${objetivo}")
                        st.write(f"El gasto total del año {anio_actual} fue de ${actual} lo que representa un `{((actual/objetivo)-1)*1:.2%}` por debajo de nuestra meta")
        #KPI 2
        with col2:
            expand = st.expander("KPI - Incrementar la tasa de asegurados en un 15%")
            with expand:
                if actual_2 > objetivo_2:
                    st.write("`KPI - Incrementar la tasa de asegurados en un 15%.`")
                    st.metric(f"Ingresos Año {anio_actual}", f"{actual_2}", delta=f"{objetivo_2}")
                    st.write(f"El objetivo para el año {anio_actual} era de {objetivo_2} asegurados")
                    st.write(f"La cantidad total de asegurados del año {anio_actual} fue de {actual_2} lo que representa un `{((actual_2/objetivo_2)-1)*1:.2%}` por encima de nuestra meta")
                elif actual_2 < objetivo_2:
                    st.write("`KPI - Incrementar la tasa de asegurados en un 15%.`")
                    st.metric(f"Gasto Año {anio_actual}", f"{actual_2}", delta=f"{objetivo_2}")
                    st.write(f"El objetivo para el año {anio_actual} era de {objetivo_2} asegurados")
                    st.write(f"La cantidad total de asegurados del año {anio_actual} fue de {actual_2} lo que representa un `{((actual_2/objetivo_2)-1)*1:.2%}` por debajo de nuestra meta")

            
        col1, col2  = st.columns(2)
        #KPI 3
        with col1:
            expand = st.expander("KPI - Incrementar la tasa de Ingresos en un 2%.")
            with expand:
                if actual_3 > objetivo_3:
                    st.write("`KPI - Incrementar la tasa de Ingresos en un 2%.`")
                    st.metric(f"Ingresos Año {anio_actual}", f"{actual_3}", delta=f"{objetivo_3}")
                    st.write(f"El objetivo para el año {anio_actual} era de ${objetivo_3}")
                    st.write(f"El ingreso total del año {anio_actual} fue de ${actual_3} lo que representa un `{((actual_3/objetivo_3)-1)*1:.2%}` por encima de nuestra meta")
                elif actual_3 < objetivo_3:
                    st.write("`KPI - Incrementar la tasa de Ingresos en un 2%.`")
                    st.metric(f"Ingresos Año {anio_actual}", f"{actual_3}", delta=f"{objetivo_3}")
                    st.write(f"El objetivo para el año {anio_actual} era de ${objetivo_3}")
                    st.write(f"El ingreso total del año {anio_actual} fue de ${actual_3} lo que representa un `{((actual_3/objetivo_3)-1)*1:.2%}` por debajo de nuestra meta")

        #KPI 4
        with col2:
            expand = st.expander("KPI - Incrementar el promedio de ingresos por asegurado en un 2%.")
            with expand:
                if actual_4 > objetivo_4:
                    st.write("`KPI - Incrementar el promedio de ingresos por asegurado en un 2%.`")
                    st.metric(f"Ingresos Año {anio_actual}", f"{actual_4}", delta=f"{objetivo_4}")
                    st.write(f"El objetivo para el año {anio_actual} era de ${objetivo_4}")
                    st.write(f"El ingreso promedio total del año {anio_actual} fue de ${actual_4} lo que representa un `{((actual_4/objetivo_4)-1)*1:.2%}` por encima de nuestra meta")
                elif actual_4 < objetivo_4:
                    st.write("`KPI - Incrementar el promedio de ingresos por asegurado en un 2%.`")
                    st.metric(f"Ingresos Año {anio_actual}", f"{actual_4}", delta=f"{objetivo_4}")
                    st.write(f"El objetivo para el año {anio_actual} era de ${objetivo_4}")
                    st.write(f"El ingreso promedio total del año {anio_actual} fue de ${actual_4} lo que representa un `{((actual_4/objetivo_4)-1)*1:.2%}` por debajo de nuestra meta")
            


        #Grafico
        #Ingreso ACTUAL vs Ingreso PASADO
        ingresos_anio_actual = df_actual[df_actual["anio"] == anio_actual].groupby("mes")["total_ingresos"].sum()
        ingresos_anio_pasado = df_previo[df_previo["anio"] == anio_previo].groupby("mes")["total_ingresos"].sum()
        conteo_mes_actual = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

        #Gasto actual vs Gasto PASADO
        egresos_anio_actual = df_actual[df_actual["anio"] == anio_actual].groupby("mes")["total_gasto"].sum()
        egresos_anio_pasado = df_previo[df_previo["anio"] == anio_previo].groupby("mes")["total_gasto"].sum()
        

        col1, col2 = st.columns(2)
       
        with col1:
            with st.expander("Ingresos"):
                i, ax = plt.subplots(figsize=(5,5))
                plt.plot(conteo_mes_actual, ingresos_anio_actual, color="green", marker="o", label=f"Ingreso año {anio_actual}")
                plt.plot(conteo_mes_actual, ingresos_anio_pasado, color="red",marker="o", label=f"Ingreso año {anio_previo}")
                plt.xlabel("Meses")
                plt.ylabel("Dinero")
                plt.title(f"Comparacion de Ingresos")
                plt.xticks(rotation=90)
                plt.legend()
                st.pyplot(i)
        
        with col2:
            with st.expander("Gastos"):
                g, ax = plt.subplots(figsize=(5,5))
                plt.plot(conteo_mes_actual, egresos_anio_actual, color="green",marker="o", label=f"Gasto año {anio_actual}")
                plt.plot(conteo_mes_actual, egresos_anio_pasado, color="red", marker="o",label=f"Gasto año {anio_previo}")
                plt.xlabel("Meses")
                plt.ylabel("Dinero")
                plt.title(f"Comparacion de Gastos")
                plt.xticks(rotation=90)
                plt.legend()
                st.pyplot(g)
        



        # col1, col2 = st.columns(2)
        # with col1:
        #     with st.expander(f"Ingresos vs Gastos Año {anio_actual}"):
        #         g, ax = plt.subplots(figsize=(5,5))
        #         plt.bar(conteo_mes_actual, ingresos_anio_actual, color="green",label=f"Ingreso año {anio_actual}")
        #         plt.bar(conteo_mes_actual, egresos_anio_actual, color="red",label=f"Gasto año {anio_previo}", bottom=g)
        #         plt.xlabel("Meses")
        #         plt.ylabel("Dinero")
        #         plt.title(f"Ingreso vs Gastos año {anio_actual}")
        #         plt.xticks(rotation=90)
        #         plt.legend()
        #         st.pyplot(g)
        
        # with col1:
        #     with st.expander(f"Ingresos vs Gastos Año {anio_previo}"):
        #         g, ax = plt.subplots(figsize=(5,5))
        #         plt.plot(conteo_mes_actual, ingresos_anio_pasado, color="green",marker="o", label=f"Ingreso año {anio_actual}")
        #         plt.plot(conteo_mes_actual, egresos_anio_pasado, color="red", marker="o",label=f"Gasto año {anio_previo}")
        #         plt.xlabel("Meses")
        #         plt.ylabel("Dinero")
        #         plt.title(f"Ingreso vs Gastos año {anio_previo}")
        #         plt.xticks(rotation=90)
        #         plt.legend()
        #         st.pyplot(g)







