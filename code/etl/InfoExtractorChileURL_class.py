# Esta clase implementa los métodos de extracción de información de Chile desde la URL de Chile

# Imports
from datetime import datetime, timedelta
import InfoExtractor_class as iex
import requests
import pandas as pd
from bs4 import BeautifulSoup



class InfoExtractorChileURL(iex.InfoExtractor):
    '''
    Esta clase implementa los métodos solicitados en InfoExtractor.
    El objetivo de esta clase es el de extraer información desde la URL de Chile.
    '''
    def _init_(self) -> None:
        super()._init_()

    def extractInfo(self, country: str, source: str, fromDateTime: datetime, toDateTime: datetime):
        '''
        Este metodo permite extraer información de un país dado utilizando source como origen de los datos
        entre dos fechas dadas.
        El source solo puedo usarlo para validar que me están solicitando extraer información desde una fuente
        que conozco en este contexto.

        '''

        # Para crear el listado de fechas:
        fechas = []
        delta = timedelta(days=1)  # Establecemos un incremento diario.
        fechaposterior = toDateTime + delta
        data = []

        # Para generar el listado de fechas:
        while fromDateTime <= fechaposterior:
            fechas.append(fromDateTime)
            fromDateTime += delta

        #Creamos una lista vacía para los datos y establecemos la ruta de la url raíz:

        url = "https://www.sismologia.cl/sismicidad/catalogo/"

        #Iteramos fecha a fecha de la lista fechas creada en el paso anterior:
        for fecha in fechas:
            # Realizar una solicitud HTTP a la página web con el formato de fecha de la url:
            num = fecha.strftime('%Y/%m/%Y%m%d.html')
            dir = url + num
            response = requests.get(dir)

            # Para crear un objeto BeautifulSoup a partir del contenido HTML obtenido:
            soup = BeautifulSoup(response.content, "html.parser")

            # Para extraer los datos de las filas:
            data_rows = soup.find_all('tr')[1:]  # Para ignorar la primera fila que contiene los encabezados.
            for row in data_rows:
                row_data = [cell.text for cell in row.find_all('td')]
                data.append(row_data)#Para agregar la información de cada fila a la lista vacía creada al comienzo.

        # Para crear el DataFrame con la información almacenada en Data:
        ChileCrudo = pd.DataFrame(data, columns=[1,2,3,4,5])

        return ('', ChileCrudo)

                