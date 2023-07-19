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


# Crear una función para ejecutar las consultas en BigQuery y obtener un DataFrame
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

# selected = option_menu(
#             menu_title=None,
#             options=["Nosotros", "Historia", "Comunidad", "Clientes", "Modelos", "Contacto" ],
#             icons=["book" ,"list-task","list-task","list-task","list-task","envelope"],
#             menu_icon="cast",
#             default_index=0,
#             orientation="horizontal",
#     styles={
#         "container": {"padding": "0!important"}})



# def historia():
def historia():
    st.subheader("Informacion relevante sobre los sismos")
    col1, col2 = st.columns(2)
    with col1:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        q_sismo = st.expander("¿Que es un Sismos?")
        with q_sismo:
            st.write("Un `sismo` es un temblor o una sacudida de la tierra por causas internas. El término es sinónimo de terremoto o seísmo, aunque en algunas regiones geográficas los conceptos de sismo o seísmo se utilizan para hacer referencia a temblores de menor intensidad que un terremoto. Una de las principales causas de los sismos es la deformación de las rocas contiguas a una falla activa, que liberan su energía potencial acumulada y producen grandes temblores. Los procesos volcánicos, los movimientos de laderas y el hundimiento de cavidades cársticas también pueden generar sismos. Estos movimientos se producen por el choque de las placas tectónicas. La colisión libera energía mientras los materiales de la corteza terrestre se reorganizan para volver a alcanzar el equilibrio mecánico. También pueden ocurrir por otras causas, como por ejemplo, impactos de asteroides o de cualquier objeto celeste de gran tamaño, o incluso pueden ser producidos por el ser humano al realizar detonaciones nucleares subterráneas.")
            if q_sismo:
                imagen = Image.open(r"Imagenes/sismo.jpg")
                st.image(imagen, "Imagen referencial de un Sismo", width=750)
    with col2:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        causas = st.expander("Causas de los sismos")
        with causas:
            st.write("Aunque la interacción entre Placas Tectónicas es la principal causa de los sismos no es la única. Cualquier proceso que pueda lograr grandes concentraciones de energía en las rocas puede generar sismos cuyo tamaño dependerá, entre otros factores, de qué tan grande sea la zona de concentración del esfuerzo.")
            st.write("Las `causas` más generales se pueden enumeran según su orden de importancia en:")
            st.write("`Tectónica`: son los sismos que se originan por el desplazamiento de las placas tectónicas que conforman la corteza, afectan grandes extensiones y es la causa que más genera sismos.")
            st.write("`Volcánica`: es poco frecuente; cuando la erupción es violenta genera grandes sacudidas que afectan sobre todo a los lugares cercanos, pero a pesar de ello su campo de acción es reducido en comparación con los de origen tectónico.")
            st.write("`Hundimiento`: cuando al interior de la corteza se ha producido la acción erosiva de las aguas subterráneas, va dejando un vacío, el cual termina por ceder ante el peso de la parte superior. Es esta caída que genera vibraciones conocidas como sismos. Su ocurrencia es poco frecuente y de poca extensión.")
            st.write("`Deslizamientos`: el propio peso de las montañas es una fuerza enorme que tiende a aplanarlas y que puede producir sismos al ocasionar deslizamientos a lo largo de fallas, pero generalmente no son de gran magnitud.")
            st.write("`Explosiones Atómicas`: realizadas por el ser humano y que al parecer tienen una relación con los movimientos sísmicos.")
    
    col1, col2 = st.columns(2)
    with col1:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        q_sismo = st.expander("Cinturón de Fuego")
        with q_sismo:
            st.write("El `cinturón de fuego` del Pacífico o anillo de fuego del Pacífico es una de las zonas de subducción ubicada en las costas del océano Pacífico caracterizada por ser algunas de las regiones sísmicas y volcánicas más importantes y activas del mundo. El lecho del océano Pacífico reposa sobre varias placas tectónicas que están en permanente fricción y por ende, acumulan tensión. Cuando esa tensión se libera, origina terremotos en los países del cinturón. Además, la zona concentra actividad volcánica constante. En esta zona las placas de la corteza terrestre se hunden a gran velocidad (varios centímetros por año) y a la vez acumulan enormes tensiones que deben liberarse en forma de sismos.")
            if q_sismo:
                imagen = Image.open(r"Imagenes/cinturon de fuego.png")
                st.image(imagen, "Cinturon de Fuego", width=750)
    with col2:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        q_sismo = st.expander("Epicentro e Hipocentro")
        with q_sismo:
            st.write("El `epicentro` es el punto en la superficie de la Tierra que se encuentra sobre la proyección vertical del hipocentro o foco, el punto del interior de la Tierra en el que se origina un terremoto. El epicentro es usualmente el lugar con mayor daño. Sin embargo, en el caso de grandes terremotos, la longitud de la ruptura de la falla puede ser muy grande, por lo que el mayor daño puede localizarse no en el epicentro, sino en cualquier otro punto de la zona de ruptura. El epicentro es usualmente el lugar con mayor daño. Sin embargo, en el caso de grandes terremotos, la longitud de la ruptura de la falla puede ser muy grande, por lo que el mayor daño puede localizarse no en el epicentro, sino en cualquier otro punto de la zona de ruptura.")
            st.write("El `hipocentro`, foco de un terremoto o foco sísmico, es el punto interior de la Tierra donde se inicia un movimiento sísmico o terremoto.​El epicentro es la proyección del hipocentro sobre la superficie terrestre, la vertical del foco; que suele ser el lugar donde el sismo se siente con mayor intensidad.​ El hipocentro es un punto del interior de la litosfera, mientras que el epicentro está en la superficie de esta.")
            if q_sismo:
                imagen = Image.open(r"Imagenes/epicentro hipocentro.png")
                st.image(imagen, "Imagen referencial de un Sismo", width=750)

        
    col1, col2 = st.columns(2)
    with col1:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        q_sismo = st.expander("Magnitud")
        with q_sismo:
            st.write("La `magnitud` de un sismo es un número que busca caracterizar el tamaño de un sismo y la energía sísmica liberada. Se mide en una escala logarítmica, de tal forma que cada unidad de magnitud corresponde a un incremento de raíz cuadrada de 1000, o bien, de aproximadamente 32 veces la energía liberada.")
            st.write("`Magnitud Local(ML)`: También conocida como escala simológica de Richter, se determina utilizando las ondas internas (ondas primarias P y secundarias S) captadas por los sismógrafos de las estaciones más cercanas al lugar en que se generó el temblor. Se puede estimar rápidamente, pero se satura para sismos grandes desde magnitud 6 hacia arriba, no reflejando el tamaño real del sismo a partir de ese valor.")
            st.write("`Magnitud de la onda corporal(mB)`: fue desarrollada para superar las limitaciones de distancia y magnitud de la escala ML inherente al uso de ondas superficiales. mB se basa en las ondas P y S, medidas durante un período más largo, y no se satura hasta alrededor de M8. Sin embargo, no es sensible a eventos menores que aproximadamente M5.5. El uso de mB como se definió originalmente ha sido abandonado en gran parte, ahora reemplazado por la escala estandarizada mBBB.")
            st.write("`Magnitud de coda(Mc)`: Esta magnitud se obtiene a partir de la duración del registro sísmico (i.e., del sismograma). La coda de un sismograma corresponde a la parte tardía de la señal que decrece monotónicamente conforme pasa el tiempo hasta alcanzar su nivel original, previo al sismo.")
            st.write("`Magnitud de ondas superficiales (Ms)`: Esta magnitud se calcula utilizando las ondas superficiales de los sismos, las que son filtradas dejando pasar solo las con períodos entre 15 y 25 segundos, de ellas se seleccionan las que poseen mayor amplitud. Uno de los problemas que genera este método es que las ondas de períodos entre 15 a 20 segundos se saturan para un sismo de magnitud cercana a 8 grados o superior, por lo que este método no permite calcular adecuadamente la magnitud de sismos mayores, lo que en estos casos obliga a utilizar otro tipo de medición.")
            st.write("`Magnitud de Momento(Mw)`: Esta fórmula permite medir un sismo calculando el tamaño de la falla (el producto del largo por su ancho) y el desplazamiento promedio que se produjo en la ruptura. ")
            if q_sismo:
                imagen = Image.open(r"Imagenes/magnitud.jpg")
                st.image(imagen, "Imagen referencial - Medicion de Magnitud", width=750)
    with col2:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        q_sismo = st.expander("Profundidad, Latitud y Longitud")
        with q_sismo:
            st.write("La `profundidad` de un sismo se refiere a la distancia vertical medida desde la superficie de la Tierra hasta el punto en el interior de la corteza terrestre donde se origina el evento sísmico, conocido como hipocentro o foco.") 
            st.write("La `latitud` proporciona la localización de un lugar al norte o al sur del Ecuador, y se expresa con medidas angulares que van desde 0° en el Ecuador hasta 90° en los polos (latitud norte /latitud sur).")
            st.write("La `longitud` representa la localización de un lugar al este o al oeste de una línea norte-sur denominada “meridiano de referencia” (Greenwich), que se mide en ángulos que van de 0° en el meridiano de origen a 180° en la línea internacional de cambio de fecha.")
            st.write("Cada grado de longitud y latitud se divide en 60 minutos y cada minuto en 60 segundos. De este modo se puede asignar una localización precisa a cualquier lugar de la Tierra.")

    col1, col2 = st.columns(2)
    with col1:
        # seleccion = st.selectbox("Selecciona la informacion que deseas consultar",options=["", "¿Que es un Sismo?", "Causas de los sismos", "Cinturon de Fuego", "Epicentro e Hipocentro", "Terminos de Sismos" ])
        q_sismo = st.expander("Terminos referentes a sismos")
        with q_sismo:
            st.write("`Ondas Sísmicas`: Las ondas sísmicas son las vibraciones que se propagan desde el epicentro de un sismo. Hay dos tipos principales de ondas sísmicas: las ondas P (primarias) y las ondas S (secundarias).")
            st.write("`P-wave (onda P)`: Las ondas P (primarias) son un tipo de onda sísmica que se propagan a través de la Tierra y son las primeras en llegar a una estación sísmica después de un terremoto. Son ondas de compresión y pueden viajar a través de sólidos, líquidos y gases.")
            st.write("`S-wave (onda S)`: Las ondas S (secundarias) son otro tipo de onda sísmica que se propagan a través de la Tierra después de un terremoto. Son ondas de corte y solo pueden viajar a través de sólidos, no a través de líquidos o gases.")
            st.write("`Ondas superficiales`: Las ondas superficiales son las ondas sísmicas que se propagan a lo largo de la superficie de la Tierra después de un sismo. Estas ondas suelen tener amplitudes más grandes y son responsables de la mayoría de los daños causados por los terremotos.")
            st.write("`Sismógrafo`: Un sismógrafo es un instrumento utilizado para medir y registrar los movimientos de la Tierra durante un sismo. Ayuda a determinar la magnitud y la duración del evento.")
            st.write("`Falla`: Una falla es una fractura en la corteza terrestre a lo largo de la cual se produce el desplazamiento de rocas. Los sismos generalmente ocurren en las fallas cuando se libera energía acumulada.")
            st.write("`Réplicas`: Las réplicas son sismos más pequeños que ocurren después de un terremoto principal. Son causados por la liberación de energía residual en la zona de la falla.")



    
            







                
                















