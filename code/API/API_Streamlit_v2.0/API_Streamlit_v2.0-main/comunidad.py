# Importar las bibliotecas necesarias
import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import db_dtypes
from PIL import Image
import time
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_option_menu import option_menu
from streamlit import *
import plost
import plotly.express as px
import seaborn as sns
import pydeck as pdk
import numpy as np
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


def run_query(query):
    # client = bigquery.Client()
    # query_job = client.query(query)
    # results = query_job.result().to_dataframe()
    # return results
    path_root = ETLEnvironment().root_project_path
    json_credentials = "project-sismos-2a770c4ff889.json"
    first_scope = "https://www.googleapis.com/auth/cloud-platform"
    credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

# Crear la aplicación Streamlit
def comunidad():
    st.header("Bienvenido al consultor de KEGV INC")
    st.write("Este consultor tiene por finalidad ayudar a la comunidad en general a entender un poco mejor la informacion sobre los sismos")
    col1, col2, col3,col4 = st.columns(4)
    with col1:
        pais = st.selectbox("Selecciona el pais a consultar", options=["","CHILE", "JAPON", "USA"])
        anio = ""
    with col1:
        if pais == "CHILE":
            anio = st.selectbox("Selecciona el año a consultar", options=["",2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023],index=0)
            with col2:
                imagen = Image.open(r"Imagenes/bandera chile.jpg")
                st.image(imagen, "Bandera de Chile", width=300)
                if anio != "":
                    with col1:
                        with st.spinner(f"Recolectando informacion de {pais} para el año {anio}..."):
                            time.sleep(3)
        elif pais == "JAPON":
            anio = st.selectbox("Selecciona el año a consultar", options=["",2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023],index=0)
            with col2:
                imagen = Image.open(r"Imagenes/bandera japon.jpg")
                st.image(imagen, "Bandera de Japon", width=300)
                if anio != " ":
                    with col1:
                        with st.spinner(f"Recolectando informacion de {pais} para el año {anio}..."):
                            time.sleep(3)
        elif pais == "USA":
            anio = st.selectbox("Selecciona el año a consultar", options=["",2018,2019,2020,2021,2022,2023], index=0)
            with col2:
                imagen = Image.open(r"Imagenes/bandera usa.jpg")
                st.image(imagen, "Bandera de Estados Unidos", width=300)
                if anio != "":
                    with col1:
                        with st.spinner(f"Recolectando informacion de {pais} para el año {anio}..."):
                            time.sleep(3)

    #Activadores
    mostrar_button = False
    mostrar_graficos = False
    mostrar_metricas = False
    #Cambiamos el nombre
    if pais == "CHILE":
        pais = "CL"
    elif pais == "JAPON":
        pais = "JP"
    elif pais == "USA":
        pais = "US"

    # Definir una consulta de ejemplo
    if pais =="" or anio == "":
        pass
    elif pais != "" and anio != "":
        query = f"SELECT * FROM `sismos_db.sismos` WHERE ID_Pais = '{pais}' and extract(year from Fecha_del_sismo) = {anio} ;"
        df = run_query(query)
        df_muestra = df.copy()
        mostrar_button = True
        #Dataframe
        if df.empty:
            pass
        elif not df.empty:
            if pais == "US":
                st.write("A continuacion podras observar una tabla donde esta toda la informacion y caracteristicas de los sismos. Desde su fecha de ocurrencia hasta el pais donde ocurrio.")
                st.write("Esta es toda la informacion que pudimos encontrar en nuestros registros.")
                st.write(f"Contamos con `{len(df)}` registros para el año `{anio}`")
                df_muestra = df_muestra.rename(columns={"Fecha_del_sismo":"Fecha del Sismo", "Hora_del_sismo": "Hora del Sismo", "Profundidad_Km": "Profundidad (KM)", "Tipo_Magnitud": "Tipo de Magnitud", "Lugar_del_Epicentro":"Lugar del Epicentro", "ID_Pais": "Pais"})
                df_muestra = df_muestra.sample(frac=0.3)
                df_muestra = df_muestra.reset_index(drop=True)
                st.dataframe(df_muestra, width=1200)
                st.write("Informacion Importante: ")
                st.write("1. Mmmm :thinking_face: viendo estos datos, sabemos que tal vez no entiendas que pueden significar")
                st.write("2. A veces nosotros tampoco pero shhh :zipper_mouth_face:")
                st.write("3. Abajo hay un boton ... Presionalo y dale vida a los datos. Te ayudara a entender mejor")
            else:
                st.write("A continuacion podras observar una tabla donde esta toda la informacion y caracteristicas de los sismos. Desde su fecha de ocurrencia hasta el pais donde ocurrio.")
                st.write("Esta es toda la informacion que pudimos encontrar en nuestros registros.")
                st.write(f"Contamos con `{len(df)}` registros para el año `{anio}`")
                df_muestra = df_muestra.rename(columns={"Fecha_del_sismo":"Fecha del Sismo", "Hora_del_sismo": "Hora del Sismo", "Profundidad_Km": "Profundidad (KM)", "Tipo_Magnitud": "Tipo de Magnitud", "Lugar_del_Epicentro":"Lugar del Epicentro", "ID_Pais": "Pais"})
                st.dataframe(df_muestra, width=1200)
                st.write("Informacion Importante: ")
                st.write("1. Mmmm :thinking_face: viendo estos datos, sabemos que tal vez no entiendas que pueden significar")
                st.write("2. A veces nosotros tampoco pero shhh :zipper_mouth_face:")
                st.write("3. Abajo hay un boton ... Presionalo y dale vida a los datos. Te ayudara a entender mejor")

            #Indices para obtener solo los registros que tengan el pais y año correspondiente.
            lista_indices = []
            for indice, elemento in enumerate(df["Fecha_del_sismo"]):
                if df["ID_Pais"][indice]== pais and df["Fecha_del_sismo"][indice].year == anio:
                    lista_indices.append(indice)

            #Lugares para obtener de Estados Unidos
            lugares_us = []
            for indice, elemento in enumerate(df.loc[lista_indices, "Lugar_del_Epicentro"]):
                inicio_usa = str(df["Lugar_del_Epicentro"][indice]).find(",")+1 
                fin_usa = len(df["Lugar_del_Epicentro"][indice])+10
                lugar_usa = str(df["Lugar_del_Epicentro"][indice])[inicio_usa:fin_usa]
                lugares_us.append(lugar_usa)

            #Lugares para obtener de Chile
            lugares_Ch = []
            for indice in lista_indices:
                inicio_CL = str(df["Lugar_del_Epicentro"][indice]).find("de")+2 
                fin_CL = len(str(df["Lugar_del_Epicentro"][indice]))
                lugar_CL = str(df["Lugar_del_Epicentro"][indice])[inicio_CL:fin_CL]
                lugares_Ch.append(lugar_CL)


        #Transformamos a Dataframe y contamos los valores para graficar.
        lugares_cl = pd.DataFrame(lugares_Ch, columns=["Lugar"])
        lugares_usa = pd.DataFrame(lugares_us, columns=["Lugar"])

        #Contamos los valores
        lugares_cl["Lugar"].value_counts()
        lugares_usa["Lugar"].value_counts()

        #Metricas
        mg_min = df.loc[lista_indices, "Magnitud"].min()
        mg_max = df.loc[lista_indices, "Magnitud"].max()
        q_sismos = len(df.loc[lista_indices, "Magnitud"])
        lugares_unicos = df.loc[lista_indices, "Lugar_del_Epicentro"]
        lugar_mas_sismos = lugares_unicos.value_counts()[0:5].sort_values(ascending=True)
        mostrar_metricas = True

        #Graficamos las metricas
        col10,col20 = st.columns([0.58, 0.42])
        with col10:
            if mostrar_metricas == True:
                button = st.button("DALE VIDA A LOS DATOS :sunglasses:")
                if button:
                    with st.spinner("Extrayendo..."):
                        time.sleep(2)
                    with st.spinner("Transformando..."):
                        time.sleep(2)
                    with st.spinner("Cargando..."):
                        time.sleep(2)
                    with st.spinner("Dando vida a los Datos :sunglasses:..."):
                        time.sleep(2)
                    st.markdown("# Metricas")
                    st.write("En este apartado podras observar informacion reducida y relevante en cuanto a los sismos. Queremos que sea lo mas facil de interpretar y que puedas tener una vision general de los sismos, lugares mas sismologicos, magnitudes, entre otra informacion que consideramos `importante`. ")
                    st.write("En caso de tener dudas sobre alguna de nuestras metricas al lado de cada `titulo` podras encontrar un :grey_question: el cual contiene informacion de ayuda para entenderla mejor.")
                    
                    if pais == "JP":
                        col1, col2, col3 = st.columns(3)
                        col1.metric("`MAGNITUD MINIMA`", f"{mg_min} M", help=f"Esta metrica nos muestra la Magnitud minima de un sismos para el año {anio}")
                        col2.metric("`MAGNITUD MAXIMA`", f"{mg_max} M", help=f"Esta metrica nos muestra la Magnitud maxima de un sismo para el año {anio}", delta_color="off")
                        col3.metric("`SISMOS POR AÑO`", f"{q_sismos}", help=f"Esta metrica nos muestra la cantidad de sismos para el año {anio}")
                        col4,col10 = st.columns([3,0.0000000000001])
                        col4.metric("`LUGAR MAS SISMOLOGICO`", lugar_mas_sismos.index[4], help=f"Esta metrica nos muestra el lugar mas sismologico para el año {anio}")
                        col5, col6= st.columns(2)
                        col5.metric("`SISMOS EN EPICENTRO`", lugar_mas_sismos[4], help=f"Esta metrica nos muestra la cantidad de sismos que ocurrieron en {lugar_mas_sismos.index[4]} para el año {anio}")
                        col6.metric("`PORCENTAJE`",f"{lugar_mas_sismos[4]/q_sismos:.2%}", help=f"Esta metrica nos muestra el porcentaje que representa la cantidad de sismos ocurridos en {lugar_mas_sismos.index[4]} en comparacio con el total" )
                        style_metric_cards(background_color="gray", border_color="white", border_left_color="cyan")

                        #Empezamos a graficar.
                        st.markdown("# Graficos")
                        col7, col8 = st.columns(2)
                        with col7:
                            fig_bar = plt.figure(figsize=(3,3))
                            plt.barh(y=lugar_mas_sismos.index, width=lugar_mas_sismos.values)
                            plt.title(f"Top 5 Epicentros")
                            plt.xlabel("Frecuencia")
                            plt.ylabel("Epicentros")
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig_bar)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de barras horizontal el cual nos estan indicando el top 5 epicentros que fueron mas afectados en Japon para el año {anio}")
                            st.write(f"Donde el eje X esta representado por la cantidad de veces que los epicentros presentaron un sismo.")
                            st.write(f"Donde el eje Y esta representado por los nombres de los epicentros que mas presentaron sismos.")
                        col7, col8 = st.columns(2)
                        with col7:
                            enero = 0
                            febrero=0
                            marzo=0
                            abril =0
                            mayo =0
                            junio=0
                            julio=0
                            agosto=0
                            septiembre=0
                            octubre=0
                            noviembre=0
                            diciembre=0

                            for indice, elemento in enumerate(df["Fecha_del_sismo"]):
                                if df["Fecha_del_sismo"][indice].year == anio:
                                    if df["Fecha_del_sismo"][indice].month == 1:
                                        enero +=1
                                    elif df["Fecha_del_sismo"][indice].month == 2:
                                        febrero +=1
                                    elif df["Fecha_del_sismo"][indice].month == 3:
                                        marzo +=1
                                    elif df["Fecha_del_sismo"][indice].month == 4:
                                        abril +=1
                                    elif df["Fecha_del_sismo"][indice].month == 5:
                                        mayo +=1
                                    elif df["Fecha_del_sismo"][indice].month == 6:
                                        junio +=1
                                    elif df["Fecha_del_sismo"][indice].month == 7:
                                        julio +=1
                                    elif df["Fecha_del_sismo"][indice].month ==8:
                                        agosto +=1
                                    elif df["Fecha_del_sismo"][indice].month == 9:
                                        septiembre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 10:
                                        octubre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 11:
                                        noviembre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 12:
                                        diciembre +=1
                            meses = {"Enero": enero, "Febrero": febrero, "Marzo":marzo, "Abril":abril, "Mayo":mayo, "Junio":junio, "Julio":julio, "Agosto":agosto, "Septiembre":septiembre, "Octubre":octubre, "Noviembre":noviembre, "Diciembre":diciembre}
                                                        
                            meses = {"Enero": enero, "Febrero": febrero, "Marzo":marzo, "Abril":abril, "Mayo":mayo, "Junio":junio, "Julio":julio, "Agosto":agosto, "Septiembre":septiembre, "Octubre":octubre, "Noviembre":noviembre, "Diciembre":diciembre}                                
                            df_meses = pd.DataFrame(meses, index=range(1))
                            fig = plt.figure()
                            plt.plot(df_meses.columns, df_meses.values[0], marker="o", ls="--", markerfacecolor = "white", color="green")
                            plt.xticks(rotation=90)
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de lineas el cual nos esta mostrando la evolucion de los sismos a traves de los meses en Japon para el año {anio}")
                            st.write(f"Donde el eje X esta representado por los nombres de los meses del año.")
                            st.write(f"Donde el eje Y esta representado por la frecuencia o acumulacion de las veces que ocurrio un sismo en el mes.")

                        col7, col8 = st.columns([0.9,0.1])
                        with col7:
                            df_map = df[["Longitud", "Latitud"]]
                            df_map = df_map.rename(columns={"Longitud":"lon", "Latitud": "lat"})
                            with st.expander("¿Quieres ver algo genial? :shocked_face_with_exploding_head:"):
                                st.markdown("# Mapa :sunglasses:")
                                st.write(f"Cada punto blanco representa un sismo que ocurrio para el año {anio}")     
                                st.pydeck_chart(pdk.Deck(
                                    #map_style="mapbox://styles/mapbox/streets-v11",
                                    map_style=None,
                                    initial_view_state=pdk.ViewState(
                                        latitude=36.204824,
                                        longitude=138.252924,
                                        zoom=7,
                                        pitch=50,
                                    ),
                                    layers=[
                                        pdk.Layer(
                                        'HexagonLayer',
                                        data=df_map,
                                        get_position='[lon, lat]',
                                        elevation_range=[0, 1000],
                                        pickable=True,
                                        extruded=True,
                                        get_radius=5,
                                        radius=-10
                                        ),
                                        pdk.Layer(
                                            'ScatterplotLayer',
                                            data=df_map,
                                            auto_highlight=True,
                                            elevation_scale=50,
                                            pickable=True,
                                            elevation_range=[0, 1000],
                                            get_position='[lon, lat]',
                                            get_color='[255,255,0, 0]',
                                            get_radius=250,
                                            border_radius=100,
                                            border_color_radius="black",
                                            extruded=True
                                        
                        ),
                    ],
                ))
                    
                    elif pais == "CL":
                        col1, col2, col3 = st.columns(3)
                        col1.metric("`MAGNITUD MINIMA`", f"{mg_min} M", help=f"Esta metrica nos muestra la Magnitud minima de un sismos para el año {anio}")
                        col2.metric("`MAGNITUD MAXIMA`", f"{mg_max} M", help=f"Esta metrica nos muestra la Magnitud maxima de un sismo para el año {anio}", delta_color="off")
                        col3.metric("`SISMOS POR AÑO`", f"{q_sismos}", help=f"Esta metrica nos muestra la cantidad de sismos para el año {anio}")
                        col4,col10 = st.columns([3,0.0000000000001])
                        col4.metric("`LUGAR MAS SISMOLOGICO`", lugar_mas_sismos.index[4], help=f"Esta metrica nos muestra el lugar mas sismologico para el año {anio}")
                        col5, col6= st.columns(2)
                        col5.metric("`SISMOS EN EPICENTRO`", lugar_mas_sismos[4], help=f"Esta metrica nos muestra la cantidad de sismos que ocurrieron en {lugar_mas_sismos.index[4]} para el año {anio}")
                        col6.metric("`PORCENTAJE`",f"{lugar_mas_sismos[4]/q_sismos:.2%}", help=f"Esta metrica nos muestra el porcentaje que representa la cantidad de sismos ocurridos en {lugar_mas_sismos.index[4]} en comparacio con el total" )
                        style_metric_cards(background_color="gray", border_color="white", border_left_color="cyan")

                        #Empezamos a graficar.
                        st.markdown("# Graficos")
                        col7, col8 = st.columns(2)
                        with col7:
                            fig_bar = plt.figure(figsize=(3,3))
                            plt.barh(y=lugar_mas_sismos.index, width=lugar_mas_sismos.values)
                            plt.title(f"Top 5 Epicentros")
                            plt.xlabel("Frecuencia")
                            plt.ylabel("Epicentros")
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig_bar)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de barras horizontal el cual nos estan indicando el top 5 epicentros que fueron mas afectados en Chile para el año {anio}")
                            st.write(f"Donde el eje X esta representado por la cantidad de veces que los epicentros presentaron un sismo.")
                            st.write(f"Donde el eje Y esta representado por los nombres de los epicentros que mas presentaron sismos.")
                        
                        col7, col8 = st.columns(2)
                        with col7:
                            enero = 0
                            febrero=0
                            marzo=0
                            abril =0
                            mayo =0
                            junio=0
                            julio=0
                            agosto=0
                            septiembre=0
                            octubre=0
                            noviembre=0
                            diciembre=0

                            for indice, elemento in enumerate(df["Fecha_del_sismo"]):
                                if df["Fecha_del_sismo"][indice].year == anio:
                                    if df["Fecha_del_sismo"][indice].month == 1:
                                        enero +=1
                                    elif df["Fecha_del_sismo"][indice].month == 2:
                                        febrero +=1
                                    elif df["Fecha_del_sismo"][indice].month == 3:
                                        marzo +=1
                                    elif df["Fecha_del_sismo"][indice].month == 4:
                                        abril +=1
                                    elif df["Fecha_del_sismo"][indice].month == 5:
                                        mayo +=1
                                    elif df["Fecha_del_sismo"][indice].month == 6:
                                        junio +=1
                                    elif df["Fecha_del_sismo"][indice].month == 7:
                                        julio +=1
                                    elif df["Fecha_del_sismo"][indice].month ==8:
                                        agosto +=1
                                    elif df["Fecha_del_sismo"][indice].month == 9:
                                        septiembre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 10:
                                        octubre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 11:
                                        noviembre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 12:
                                        diciembre +=1
                            meses = {"Enero": enero, "Febrero": febrero, "Marzo":marzo, "Abril":abril, "Mayo":mayo, "Junio":junio, "Julio":julio, "Agosto":agosto, "Septiembre":septiembre, "Octubre":octubre, "Noviembre":noviembre, "Diciembre":diciembre}
                                                        
                            meses = {"Enero": enero, "Febrero": febrero, "Marzo":marzo, "Abril":abril, "Mayo":mayo, "Junio":junio, "Julio":julio, "Agosto":agosto, "Septiembre":septiembre, "Octubre":octubre, "Noviembre":noviembre, "Diciembre":diciembre}                                
                            df_meses = pd.DataFrame(meses, index=range(1))
                            fig = plt.figure()
                            plt.plot(df_meses.columns, df_meses.values[0], marker="o", ls="--", markerfacecolor = "white", color="green")
                            plt.xticks(rotation=90)
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig)                    
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de lineas el cual nos esta mostrando la evolucion de los sismos a traves de los meses en Chile para el año {anio}")
                            st.write(f"Donde el eje X esta representado por los nombres de los meses del año.")
                            st.write(f"Donde el eje Y esta representado por la frecuencia o acumulacion de las veces que ocurrio un sismo en el mes.")

                        col7, col8 = st.columns(2)
                        with col7:
                            fig_bar = plt.figure(figsize=(3,3))
                            plt.bar(x=lugares_cl["Lugar"].value_counts().index[0:5], height=lugares_cl["Lugar"].value_counts().values[0:5])
                            plt.title(f"Top 5 ciudades sismicas")
                            plt.xlabel("Ciudades")
                            plt.ylabel("Frecuencia")
                            plt.xticks(rotation=90)
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig_bar)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de barras el cual nos estan indicando el top 5 ciudades que fueron mas afectados en Chile para el año {anio}")
                            st.write(f"Donde el eje X esta representado por los nombres de los lugares que mas presentaron sismos.")
                            st.write(f"Donde el eje Y esta representado por la cantidad de veces que las ciudades presentaron un sismo.")

                        col7, col8 = st.columns([0.9,0.1])
                        with col7:
                            df_map = df[["Longitud", "Latitud"]]
                            df_map = df_map.rename(columns={"Longitud":"lon", "Latitud": "lat"})
                            with st.expander("¿Quieres ver algo genial? :shocked_face_with_exploding_head:"):
                                st.markdown("# Mapa :sunglasses:")
                                st.write(f"Cada punto blanco representa un sismo que ocurrio para el año {anio}")     
                                st.pydeck_chart(pdk.Deck(
                                    #map_style="mapbox://styles/mapbox/streets-v11",
                                    map_style=None,
                                    initial_view_state=pdk.ViewState(
                                        latitude=-33.45694,
                                        longitude=-70.64827,
                                        zoom=8,
                                        pitch=50,
                                    ),
                                    layers=[
                                        pdk.Layer(
                                        'HexagonLayer',
                                        data=df_map,
                                        get_position='[lon, lat]',
                                        elevation_range=[0, 1000],
                                        pickable=True,
                                        extruded=True,
                                        get_radius=5,
                                        radius=-10
                                        ),
                                        pdk.Layer(
                                            'ScatterplotLayer',
                                            data=df_map,
                                            auto_highlight=True,
                                            elevation_range=[0, 1000],
                                            get_position='[lon, lat]',
                                            get_color='[255,255,255, 255]',
                                            get_radius=250
                                        
                        ),
                    ],
                ))

                    elif pais == "US":
                        col1, col2, col3 = st.columns(3)
                        col1.metric("`MAGNITUD MINIMA`", f"{mg_min} M", help=f"Esta metrica nos muestra la Magnitud minima de un sismos para el año {anio}")
                        col2.metric("`MAGNITUD MAXIMA`", f"{mg_max} M", help=f"Esta metrica nos muestra la Magnitud maxima de un sismo para el año {anio}", delta_color="off")
                        col3.metric("`SISMOS POR AÑO`", f"{q_sismos}", help=f"Esta metrica nos muestra la cantidad de sismos para el año {anio}")
                        col4,col10 = st.columns([3,0.0000000000001])
                        col4.metric("`LUGAR MAS SISMOLOGICO`", lugar_mas_sismos.index[4], help=f"Esta metrica nos muestra el lugar mas sismologico para el año {anio}")
                        col5, col6= st.columns(2)
                        col5.metric("`SISMOS EN EPICENTRO`", lugar_mas_sismos[4], help=f"Esta metrica nos muestra la cantidad de sismos que ocurrieron en {lugar_mas_sismos.index[4]} para el año {anio}")
                        col6.metric("`PORCENTAJE`",f"{lugar_mas_sismos[4]/q_sismos:.2%}", help=f"Esta metrica nos muestra el porcentaje que representa la cantidad de sismos ocurridos en {lugar_mas_sismos.index[4]} en comparacio con el total" )
                        style_metric_cards(background_color="gray", border_color="black", border_left_color="cyan")
                        #Empezamos a graficar
                        st.markdown("# Graficos")
                        col7, col8 = st.columns(2)
                        with col7:
                            fig_bar = plt.figure(figsize=(3,3))
                            plt.barh(y=lugar_mas_sismos.index, width=lugar_mas_sismos.values)
                            plt.title(f"Top 5 Epicentros")
                            plt.xlabel("Frecuencia")
                            plt.ylabel("Epicentros")
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig_bar)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de barras horizontal el cual nos estan indicando el top 5 epicentros que fueron mas afectados en Estados Unidos para el año {anio}")
                            st.write(f"Donde el eje X esta representado por la cantidad de veces que los epicentros presentaron un sismo.")
                            st.write(f"Donde el eje Y esta representado por los nombres de los epicentros que mas presentaron sismos.")
                        

                        col7, col8 = st.columns(2)
                        with col7:
                            enero = 0
                            febrero=0
                            marzo=0
                            abril =0
                            mayo =0
                            junio=0
                            julio=0
                            agosto=0
                            septiembre=0
                            octubre=0
                            noviembre=0
                            diciembre=0

                            for indice, elemento in enumerate(df["Fecha_del_sismo"]):
                                if df["Fecha_del_sismo"][indice].year == anio:
                                    if df["Fecha_del_sismo"][indice].month == 1:
                                        enero +=1
                                    elif df["Fecha_del_sismo"][indice].month == 2:
                                        febrero +=1
                                    elif df["Fecha_del_sismo"][indice].month == 3:
                                        marzo +=1
                                    elif df["Fecha_del_sismo"][indice].month == 4:
                                        abril +=1
                                    elif df["Fecha_del_sismo"][indice].month == 5:
                                        mayo +=1
                                    elif df["Fecha_del_sismo"][indice].month == 6:
                                        junio +=1
                                    elif df["Fecha_del_sismo"][indice].month == 7:
                                        julio +=1
                                    elif df["Fecha_del_sismo"][indice].month ==8:
                                        agosto +=1
                                    elif df["Fecha_del_sismo"][indice].month == 9:
                                        septiembre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 10:
                                        octubre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 11:
                                        noviembre +=1
                                    elif df["Fecha_del_sismo"][indice].month == 12:
                                        diciembre +=1
                            
                            meses = {"Enero": enero, "Febrero": febrero, "Marzo":marzo, "Abril":abril, "Mayo":mayo, "Junio":junio, "Julio":julio, "Agosto":agosto, "Septiembre":septiembre, "Octubre":octubre, "Noviembre":noviembre, "Diciembre":diciembre}                                
                            df_meses = pd.DataFrame(meses, index=range(1))
                            fig = plt.figure()
                            plt.plot(df_meses.columns, df_meses.values[0], marker="o", ls="--", markerfacecolor = "white", color="green")
                            plt.xticks(rotation=90)
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de lineas el cual nos esta mostrando la evolucion de los sismos a traves de los meses en Estados Unidos para el año {anio}")
                            st.write(f"Donde el eje X esta representado por los nombres de los meses del año.")
                            st.write(f"Donde el eje Y esta representado por la frecuencia o acumulacion de las veces que ocurrio un sismo en el mes.")

                        col7, col8 = st.columns(2)
                        with col7:
                            fig_bar = plt.figure(figsize=(3,3))
                            plt.bar(x=lugares_usa["Lugar"].value_counts().index[0:5], height=lugares_usa["Lugar"].value_counts().values[0:5])
                            plt.title(f"Top 5 Ciudades")
                            plt.xlabel("Ciudades")
                            plt.ylabel("Frecuencia")
                            plt.xticks(rotation=90)
                            plt.grid(visible=True, ls = "--", alpha=0.5)
                            st.pyplot(fig_bar)
                        with col8:
                            st.markdown("### Interpretacion:")
                            st.write(f"Al lado izquierdo encontramos un grafico de barras el cual nos estan indicando el top 5 ciudades que fueron mas afectados en Estados Unidos para el año {anio}")
                            st.write(f"Donde el eje X esta representado por los nombres de los lugares que mas presentaron sismos.")
                            st.write(f"Donde el eje Y esta representado por la cantidad de veces que las ciudades presentaron un sismo.")

                        col7, col8 = st.columns([0.9,0.1])
                        with col7:
                            df_map = df[["Longitud", "Latitud"]]
                            df_map = df_map.rename(columns={"Longitud":"lon", "Latitud": "lat"})
                            with st.expander("¿Quieres ver algo genial? :shocked_face_with_exploding_head:"):
                                st.markdown("# Mapa :sunglasses:")
                                st.write(f"Cada punto blanco representa un sismo que ocurrio para el año {anio}")     
                                st.pydeck_chart(pdk.Deck(
                                #map_style="mapbox://styles/mapbox/streets-v11",
                                map_style=None,
                                initial_view_state=pdk.ViewState(
                                    latitude=38.89511,
                                    longitude=-77.03637,
                                    zoom=7,
                                    pitch=50,
                                ),
                                layers=[
                                    pdk.Layer(
                                    'HexagonLayer',
                                    data=df_map,
                                    get_position='[lon, lat]',
                                    elevation_range=[0, 1000],
                                    pickable=True,
                                    extruded=True,
                                    get_radius=5,
                                    radius=-10
                                    ),
                                    pdk.Layer(
                                        'ScatterplotLayer',
                                        data=df_map,
                                        auto_highlight=True,
                                        elevation_range=[0, 1000],
                                        get_position='[lon, lat]',
                                        get_color='[255,255,255, 255]',
                                        get_radius=250
                    ),
                ],
            ))
                               
