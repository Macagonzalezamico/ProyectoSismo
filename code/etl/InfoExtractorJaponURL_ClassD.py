# Esta clase implementa los métodos de extracción de información de Chile desde la URL de Chile

# Imports
import datetime as dt
from datetime import datetime, timedelta
import InfoExtractor_class as iex
import requests
import pandas as pd
from bs4 import BeautifulSoup as soup
import selenium
from selenium import webdriver
import time


class InfoExtractorJaponURL(iex.InfoExtractor):
    '''
    Esta clase implementa los métodos solicitados en InfoExtractor.
    El objetivo de esta clase es el de extraer información desde la URL de Chile.
    '''
    def _init_(self) -> None:
        super()._init_()

    def extractInfo(self, country: str, source: str, fromDateTime: datetime, toDateTime: datetime ):

        '''
        Este metodo permite extraer información de un país dado utilizando source como origen de los datos
        entre dos fechas dadas.
        El source solo puedo usarlo para validar que me están solicitando extraer información desde una fuente
        que conozco en este contexto.

        # # '''

        lista_fechas_incremental = []
        delta = timedelta(days=1)
        while fromDateTime <= toDateTime:
            lista_fechas_incremental.append(fromDateTime)
            fromDateTime += delta

        browser = webdriver.Chrome() #Cargamos el driver a una variable
        myUrl = "https://www.data.jma.go.jp/multi/quake/?lang=es" #Url de la pagina donde vamos a ingresar
        browser.get(myUrl) #Abrimos Chrome
        time.sleep(5)
        pageSoup = soup(browser.page_source, "html.parser") #Convertimos nuestro archivo a HTML.
        pages = pageSoup.find("table", class_="quakeindex_table") #Encontramos el tag y la clase que nos interesa obtener.
        links = pages.find_all("a") #Encontramos el tag.

        #Creamos la lista de fechas
        lista_fechas = []
        for indice, elemento in enumerate(links):
            inicio = str(links[indice]).index(">")+1 #extraemos el final del link
            fin = inicio +10
            fecha = str(links[indice])[inicio:fin]
            lista_fechas.append(fecha)

        #Convertir la lista de fechas
        lista_fechas = [datetime.strptime(fecha_str, '%Y/%m/%d') for fecha_str in lista_fechas]
        lista_fechas_incremental = [fecha.strftime('%Y-%m-%d') for fecha in lista_fechas_incremental]

        #Indices
        indices = []
        for indice, elemento in enumerate(lista_fechas):
            if lista_fechas[indice].strftime('%Y-%m-%d') in lista_fechas_incremental:
                indices.append(indice)
            else:
                pass
        
        #Lista Links
        lista_links = []
        for indice, elemento in enumerate(indices):
            lista_links.append(links[indice])

        #INCREMENTAL
        lista_incremental = []
        for indice, elemento in enumerate(lista_links): #iteramos en los links
            if "quake" in str(links[indice]): 
                inicio = str(links[indice]).index("quake") #Extraemos el inicio del link
                fin = str(links[indice]).index(">")-1 #extraemos el final del link
                link = str(links[indice])[inicio:fin] #Obtenemos los links
                link = str(link).replace("amp;", "") #Reemplazamos para obtener el link limpio
                lista_incremental.append(link) #Agregamos a la lista vacia


        #DATOS
        datos = [] #Lista vacia para almacenar los datos de cada fecha
        for indice in range(len(lista_incremental)): #iteramos sobre la lista de links
            url_unico = "https://www.data.jma.go.jp/multi/quake/"+lista_incremental[indice] #concatenamos la informacionn de cada elemento con el url.
            browser_2 = webdriver.Chrome() #Cargamos el driver a una variable
            myUrl = url_unico #Url de Yapo para venta de celulares en la region metropolitana
            browser_2.get(myUrl) #Abrimos Chrome
            time.sleep(5) 
            pageSoup = soup(browser_2.page_source, "html.parser") #Convertimos nuestro archivo a HTML.
            pages = pageSoup.find("table", class_="quakeindex_table") #Encontramos el tag y la clase que nos interesa obtener.
            tr = pages.find_all("td")  #Encontramos el tag.
            datos.append(tr) #Agregamos a la lista vacia

        # Creamos la tabla donde ira la informacion
        JaponCrudo = pd.DataFrame(0, index=range(len(datos)),columns=["Hora y dia del Sismo", "Latitud", "Longitud", "Magnitud", "Profundidad del Hipocentro", "Lugar del Epicentro"])

        #POBLAMOS LA TABLA
        #iteramos para reemplazar las tags que trae del web scraping.
        for indice, elemento in enumerate(datos): 
            JaponCrudo["Hora y dia del Sismo"][indice] = datos[indice][0]
            JaponCrudo["Latitud"][indice] = datos[indice][1]
            JaponCrudo["Longitud"][indice] = datos[indice][2]
            JaponCrudo["Magnitud"][indice]= datos[indice][3]
            JaponCrudo["Profundidad del Hipocentro"][indice] = datos[indice][4]
            JaponCrudo["Lugar del Epicentro"][indice] = datos[indice][5]

            
        return ('', JaponCrudo)
