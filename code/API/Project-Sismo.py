import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from ETLEnvironment_class import ETLEnvironment
import pandas as pd
import numpy as np
import googlemaps
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os

apikey = os.environ.get('apikey')

def main(latitud, longitud, distancia_km):
    # Título de la aplicación

    def get_google_cloud_client():
        '''
        Esta función devuelve un cliente de Google Cloud listo para ser utilizado en el proyecto sismos
        '''

        # Preparo el path y scope para recuperar las credenciales
        path_root = ETLEnvironment().root_project_path
        json_credentials = "project-sismos-2a770c4ff889.json"
        first_scope = "https://www.googleapis.com/auth/cloud-platform"
        credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

        return client

    client = get_google_cloud_client()

    st.title('Resultados')

    lista = []

    # Ejecutar la consulta en BigQuery
    query = f'''
        SELECT * FROM `sismos_db.sismos`
        WHERE ST_DWITHIN(ST_GeogPoint(Longitud, Latitud), ST_GeogPoint({longitud}, {latitud}), {distancia_km} * 1000)
    '''
    job = client.query(query)
    # Mostrar los resultados en Streamlit
    for row in job.result():
        lista.append(row)

    fechas = []
    horas = []
    latitudes = []
    longitudes = []
    profundidades = []
    magnitudes = []
    tipo_magnitudes = []
    epicentros = []
    id_paises = []
    for fila in lista:
        fechas.append(fila[0])
        horas.append(fila[1])
        latitudes.append(fila[2])
        longitudes.append(fila[3])
        profundidades.append(fila[4])
        magnitudes.append(fila[5])
        tipo_magnitudes.append(fila[6])
        epicentros.append(fila[7])
        id_paises.append(fila[8])

    data = {
        'Fecha_del_sismo': fechas,
        'Hora_del_sismo': horas,
        'Latitud': latitudes,
        'Longitud': longitudes,
        'Profundidad_Km': profundidades,
        'Magnitud': magnitudes,
        'Tipo_Magnitud': tipo_magnitudes,
        'Lugar_del_Epicentro': epicentros,
        'ID_Pais': id_paises
    }

    df = pd.DataFrame(data)

    ProfundidadPromedio = df["Profundidad_Km"].mean()
    ProfundidadPromedio = round(ProfundidadPromedio, 2)
    ProfundidadMaxima = df["Profundidad_Km"].max()
    ProfundidadMinima = df["Profundidad_Km"].min()
    MagnitudPromedio = df["Magnitud"].mean()
    MagnitudPromedio = round(MagnitudPromedio, 2)
    MagnitudMaxima = df["Magnitud"].max()
    MagnitudMinima = df["Magnitud"].min()
    Cantidad = len(df)
    
    df["Magnitud"] = df["Magnitud"].astype(float)
    countm55 = df[df["Magnitud"] > 5]["Magnitud"].count()
    countm65 = df[df["Magnitud"] > 6.5]["Magnitud"].count()
    count = len(df)
    probm55 = (countm55 / count).round(8)
    probm65 = (countm65 / count).round(8)


    if len(df) == 0:
        st.write("No contamos con registros de sismos en tu área, recuerda que esta API sólo es funcional en Chile, Japón y EEUU.")
    else:
        df['Fecha_del_sismo'] = pd.to_datetime(df['Fecha_del_sismo'])

        # Calcular la cantidad de sismos por día
        sismos_por_dia = df.groupby(df['Fecha_del_sismo'].dt.date).size().reset_index(name='Cantidad de Sismos')
        
        if len(sismos_por_dia) <= 1:
           st.write("El área es demasiado chica, no hay suficientes datos para ajustar el modelo.")
        else:
            fecha_actual = datetime.now().date()

            sismos_por_dia = sismos_por_dia[sismos_por_dia['Fecha_del_sismo']>fecha_actual-timedelta(days=365)]
            
            model = ARIMA(sismos_por_dia['Cantidad de Sismos'], order=(5, 0, 0))
            model_fit = model.fit()


            # Obtener los últimos tres meses previos a la fecha actual
            fecha_tres_meses_atras = fecha_actual - timedelta(days=90)
            ultimos_tres_meses = sismos_por_dia[(sismos_por_dia['Fecha_del_sismo'] >= fecha_tres_meses_atras)]

            # Generar las fechas para la predicción de los próximos tres meses desde la fecha actual
            fecha_tres_meses_adelante = fecha_actual + timedelta(days=90)
            fechas_prediccion = pd.date_range(start=fecha_actual, end=fecha_tres_meses_adelante, freq='D')
            prediccion = model_fit.predict(start=len(ultimos_tres_meses), end=len(ultimos_tres_meses) + len(fechas_prediccion) - 1)

            # Crear una lista con las fechas de los últimos tres meses y las fechas de predicción
            fechas = list(ultimos_tres_meses['Fecha_del_sismo']) + list(fechas_prediccion)

            # Crear una lista con la cantidad de sismos de los últimos tres meses y las predicciones
            cantidad_sismos = list(ultimos_tres_meses['Cantidad de Sismos']) + list(prediccion)

            # Crear el DataFrame con los datos de los últimos tres meses y las predicciones
            datos_grafico = pd.DataFrame({'Fecha': fechas, 'Cantidad de Sismos': cantidad_sismos})

            # Configurar la aplicación de Streamlit
            st.write('Gráfico de actividad sísmica de los últimos 3 meses y predicción de actividad para los próximos 3 meses:')
            st.line_chart(datos_grafico.set_index('Fecha'))
            
            sismos_predichos = datos_grafico.iloc[-len(fechas_prediccion):]['Cantidad de Sismos'].sum().astype(int)
            prob1 = (probm55*sismos_predichos).round(5)
            prob2 = (probm65*sismos_predichos).round(5)
            st.write(f'Según nuestros pronósticos, en tu área se registrarán una cantidad de {sismos_predichos} sismos en los próximos 3 meses, y la probabilidad de que ocurra un sismo dañino es de:')
            st.markdown("<h2 style='font-size:13px;'>(Esta estimación se hace en base al registro histórico de sismos ocurridos en el área desde el año 2000 a la actualidad, se tiene en cuenta la cantidad de sismos de magnitud superior a la dicha ocurrieron en el lugar y no cerciora eventos futuros)</h2>", unsafe_allow_html=True)
            st.write(f'Moderadamente dañino(Mg5 o superior): {prob1}%')
            st.write(f'Muy dañino(Mg6.5 o superior): {prob2}%')
            st.write("")
            st.write("Los datos históricos según nuestros registros son los siguientes:")
            st.write(f'La profundidad promedio es {ProfundidadPromedio}, y oscila entre {ProfundidadMinima} y {ProfundidadMaxima}.')
            st.write(f'La magnitud promedio es {MagnitudPromedio}, y oscila entre {MagnitudMinima} y {MagnitudMaxima}.')
            st.write(f'Contamos con registros de un total de {Cantidad} sismos en tu zona:')
            return st.dataframe(df)

# Título de la aplicación
st.title('Project-Sismo')

def opcion1():
    # Agregar contenido a la aplicación
    st.write('Ingresa latitud, longitud y radio de la zona de la que quieres conocer la actividad sismica.')

    # Ejemplo de widget interactivo
    latitud = st.text_input('Ingresa la latitud')
    st.write(f'La latitud que ingresaste es: {latitud}')

    longitud = st.text_input('Ingresa la longitud')
    st.write(f'La longitud que ingresaste es: {longitud}')

    distancia_km = st.text_input('Ingresa la distancia del radio a tener en cuenta en Km')
    st.write(f'La distancia que ingresaste es: {distancia_km} km')
    
    if st.button('Ejecutar consulta'):
        # Llamar a la función para ejecutar la consulta 
        try:
            latitud = float(latitud)
            longitud = float(longitud)
            distancia_km = float(distancia_km)
            main(latitud, longitud, distancia_km)
        except (ValueError, UnboundLocalError):
            st.write("Los parámetros ingresados son incorrectos.")


def opcion2():
    gmaps = googlemaps.Client(key=apikey)

    lugar = st.text_input("Ingrese el nombre del lugar y el país (Chile, Japon o EEUU):")

    if lugar:
        # Realizar la solicitud de geocodificación inversa
        resultados = gmaps.geocode(lugar)

        if resultados:
            # Obtener las coordenadas del primer resultado
            coordenadas = resultados[0]['geometry']['location']
            latitud = coordenadas['lat']
            longitud = coordenadas['lng']

            st.write(f"Las coordenadas del lugar '{lugar}' son:")
            st.write(f"Latitud: {latitud}")
            st.write(f"Longitud: {longitud}")
    else:
        st.write("No se encontraron resultados para el lugar ingresado.")

    distancia_km = st.text_input('Ingresa la distancia del radio a tener en cuenta en Km')
    st.write(f'La distancia que ingresaste es: {distancia_km} km')

    if st.button('Ejecutar consulta'):
        # Llamar a la función para ejecutar la consulta 
        try:
            latitud = float(latitud)
            longitud = float(longitud)
            distancia_km = float(distancia_km)
            main(latitud, longitud, distancia_km)
        except (ValueError, UnboundLocalError):
            st.write("Los parámetros ingresados son incorrectos.")

def main2():
    st.write("Seleccione en el menú de la izquierda la opción que desea utilizar")

    # Opciones del menú
    opciones = ["Presentación", "Con Coordenadas", "Con Google Maps"]
    seleccion = st.sidebar.selectbox("Selecciona un método para obtener los datos", opciones)

    # Lógica de las opciones seleccionadas
    if seleccion == "Con Coordenadas":
        opcion1()
    elif seleccion == "Con Google Maps":
        opcion2()

main2()