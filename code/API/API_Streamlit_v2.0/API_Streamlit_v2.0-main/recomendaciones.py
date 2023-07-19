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
  
def recomendaciones():
    st.subheader("Recomendaciones - Intensidad")
    st.write("Este apartado es un simulador de intensidad de sismos, para darle una vista general al publico asi como entregarle las herramientas para estar un poco mas preparados a la hora de combatir un futuro sismo.")
    col1, col2 = st.columns(2)
    with col1:
        st.header(" ")   
        selec = st.selectbox("Selecciona la Intensidad del sismo", options=["",0,1,2,3,4,"5-","5+","6-","6+",7], index=0)
    with col1:
        if selec == 0:
            image = Image.open(r"Imagenes/r_sismo0.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Imperceptible para los humanos")
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")

    with col1:
        if selec == 1:
            image = Image.open(r"Imagenes/r_sismo1.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Solo algunas personas pueden percibirlo")  
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")

    with col1:
        if selec == 2:
            image = Image.open(r"Imagenes/r_sismo2.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("La mayoria de las personas puede percibirlo")                
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")  
                    st.write("Manten la calma durante el temblor, el sismo pasara y no dejara ningun daño material o fisico")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                                        
    with col1:
        if selec == 3:
            image = Image.open(r"Imagenes/r_sismo3.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Casi todas las personas puede percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")  
                    st.write("Manten la calma durante el temblor, el sismo pasara y no dejara ningun daño material o fisico")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                    
    with col1:
        if selec == 4:
            image = Image.open(r"Imagenes/r_sismo4.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Todas las personas pueden percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")  
                    st.write("Intenta de mantener la calma durante el temblor, puede ser que objetos colgantes se mueven violentamente.")
                    st.write("Busca un lugar seguro como debajo de la mesa para protegerte de los objetos colgantes")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato, como grietas en las paredes o daños menores.")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                    
    with col1:
        if selec == "5-":
            image = Image.open(r"Imagenes/r_sismo5-.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Todas las personas pueden percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")
                    st.write("Intenta de mantener la calma durante el temblor, puede ser que objetos colgantes se mueven violentamente.")
                    st.write("Busca un lugar seguro como debajo de la mesa para protegerte de los objetos colgantes")
                    st.write("Debido a la magnitud, busca apoyarte de objetos firmes como una pared")
                    st.write("Alejate de los estantes u objetos que por el temblor puedan caer al piso y hacerte daño")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato, como grietas en las paredes o daños menores.")
                    st.write("Inspecciona tu hogar y lugar de trabajo para detectar daños estructurales.")
                    st.write("Verifica si hay fugas de gas, agua o daños en las líneas eléctricas.")
                    st.write("Si encuentras daños significativos o problemas con los servicios públicos, comunícalo a las autoridades.")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                      
        elif selec == "5+":
            image = Image.open(r"Imagenes/r_sismo5+.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Todas las personas pueden percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")
                    st.write("Intenta de mantener la calma durante el temblor, objetos colgantes se mueven violentamente.")
                    st.write("Busca un lugar seguro como debajo de la mesa para protegerte de los objetos colgantes")
                    st.write("Busca apoyarte de objetos firmes como una pared ya que debido a la intensidad se dificultara moverte")
                    st.write("Alejate de los estantes u objetos que por el temblor puedan caer al piso y hacerte daño")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato, como grietas en las paredes o daños menores.")
                    st.write("Inspecciona tu hogar y lugar de trabajo para detectar daños estructurales.")
                    st.write("Verifica si hay fugas de gas, agua o daños en las líneas eléctricas.")
                    st.write("Si encuentras daños significativos o problemas con los servicios públicos, comunícalo a las autoridades.")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
    with col1:
        if selec == "6-":
            image = Image.open(r"Imagenes/r_sismo6-.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Todas las personas pueden percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")
                    st.write("Intenta de mantener la calma durante el temblor, objetos colgantes se mueven violentamente.")
                    st.write("Busca un lugar seguro como debajo de la mesa para protegerte de los objetos colgantes")
                    st.write("Busca apoyarte de objetos firmes como una pared ya que debido a la intensidad se dificultara moverte")
                    st.write("Alejate de los estantes u objetos que por el temblor puedan caer al piso y hacerte daño")
                    st.write("Mantente alejado de las ventanas o vidrios ya que podrian quebrarse y caerse")
                    st.write("Si vives en una casa o departamento sismico busca refugio bajo la mesa o algun objeto que pueda protegerte. La propiedad sufrira daños de media categoria")
                    st.write("Si vives en una casa o departamento sismico busca refugio bajo la mesa o algun objeto que pueda protegerte. La propiedad sufrira daños de menor categoria")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato, como grietas en las paredes o daños menores.")
                    st.write("Inspecciona tu hogar y lugar de trabajo para detectar daños estructurales.")
                    st.write("Verifica si hay fugas de gas, agua o daños en las líneas eléctricas.")
                    st.write("Si encuentras daños significativos o problemas con los servicios públicos, comunícalo a las autoridades.")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                    st.write("Evalúa cuidadosamente la integridad de los edificios antes de reingresar y utiliza linternas para evitar riesgos eléctricos.") 
                    st.write("Escucha la radio o sigue las redes sociales para obtener información actualizada sobre la situación y las instrucciones de las autoridades.")     
        elif selec == "6+":
            image = Image.open(r"Imagenes/r_sismo6+.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Todas las personas pueden percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")
                    st.write("Intenta de mantener la calma durante el temblor, objetos colgantes se mueven violentamente.")
                    st.write("Busca un lugar seguro como debajo de la mesa para protegerte de los objetos colgantes")
                    st.write("Alejate de los estantes u objetos que por el temblor puedan caer al piso y hacerte daño")
                    st.write("Mantente alejado de las ventanas o vidrios ya que podrian quebrarse y caerse")
                    st.write("Si vives en una casa o departamento sismico busca refugio bajo la mesa o algun objeto que pueda protegerte. La propiedad sufrira daños considerables")
                    st.write("Si vives en una casa o departamento anti-sismos busca refugio bajo la mesa o algun objeto que pueda protegerte. La propiedad sufrira daños de menor categoria")
                    st.write("Busca refugio debajo de la mesa, ya que la gran mayoria de los objetos se desplazaran y algunos caeran")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato, como grietas en las paredes o daños menores.")
                    st.write("Inspecciona tu hogar y lugar de trabajo para detectar daños estructurales.")
                    st.write("Verifica si hay fugas de gas, agua o daños en las líneas eléctricas.")
                    st.write("Si encuentras daños significativos o problemas con los servicios públicos, comunícalo a las autoridades.")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                    st.write("Evalúa cuidadosamente la integridad de los edificios antes de reingresar y utiliza linternas para evitar riesgos eléctricos.") 
                    st.write("Escucha la radio o sigue las redes sociales para obtener información actualizada sobre la situación y las instrucciones de las autoridades.")
    with col1:
        if selec == 7:
            image = Image.open(r"Imagenes/r_sismo7.png")
            st.image(image, f"Sismo de Magnitud {selec}", width=770)
            with col2:
                st.subheader(f"Recomendaciones")
                with st.expander("Como prepararse para un sismo"):
                    st.write("Siempre manten una mochila de emergencia con un kit basico de seguridad. ")
                    st.write("Los elementos de un `Kit Básico de Seguridad` son:")
                    st.write("1. Agua (2 litros por persona al día)"), st.write("2. Comida enlatada "), st.write("3. Barras energéticas y comida deshidratada"), st.write("4. Abrelatas manual")
                    st.write("5. Linternas y pilas"), st.write("6. Radio portátil con baterías adicionales"), st.write("7. Botiquín de Primeros Auxilios"), st.write("8. Medicamentos")
                    st.write("9. Anteojos"), st.write("10. Considera las necesidades de niños, tercera edad y discapacitados"), st.write("11. Llaves de repuesto de tu casa y auto"), st.write("12. Dinero en efectivo")
                with st.expander("¿Perceptible?"):
                    st.write("Todas las personas pueden percibirlo")                    
                with st.expander(f"Que hacer ante un sismo de Intensidad {selec}"):
                    st.write("`RECUERDA SIEMPRE TENER TU MOCHILA DE EMERGENCIA A LA MANO`")
                    st.write("Intenta de mantener la calma durante el temblor, objetos colgantes se mueven violentamente.")
                    st.write("Busca un lugar seguro como debajo de la mesa para protegerte de los objetos colgantes")
                    st.write("Alejate de los estantes u objetos que por el temblor puedan caer al piso y hacerte daño")
                    st.write("Mantente alejado de las ventanas o vidrios ya que podrian quebrarse y caerse")
                    st.write("Si vives en una casa o departamento sismico busca salir del mismo. La propiedad sufrira daños totales")
                    st.write("Si vives en una casa o departamento anti-sismos busca refugio bajo la mesa o algun objeto que pueda protegerte. La propiedad sufrira daños de mediana categoria")
                    st.write("Busca refugio fuera de casa, ya que la gran mayoria de los objetos se desplazaran y caeran")
                with st.expander(f"Que hacer despues de un sismo de intensidad {selec}"):
                    st.write("Verifica si hay daños en tu entorno inmediato, como grietas en las paredes o daños menores.")
                    st.write("Inspecciona tu hogar y lugar de trabajo para detectar daños estructurales.")
                    st.write("Verifica si hay fugas de gas, agua o daños en las líneas eléctricas.")
                    st.write("Si encuentras daños significativos o problemas con los servicios públicos, comunícalo a las autoridades.")
                    st.write("Permanece alerta ante posibles réplicas y sigue las noticias y actualizaciones de las autoridades locales.")
                    st.write("Evalúa cuidadosamente la integridad de los edificios antes de reingresar y utiliza linternas para evitar riesgos eléctricos.") 
                    st.write("Escucha la radio o sigue las redes sociales para obtener información actualizada sobre la situación y las instrucciones de las autoridades.")
                    st.write("Ten precaución con posibles derrumbes, deslizamientos de tierra o avalanchas que podrían haber sido desencadenados por el sismo.")
                    st.write("Prepárate para réplicas intensas y sigue las indicaciones de las autoridades sobre evacuaciones o albergues temporales.")

   
