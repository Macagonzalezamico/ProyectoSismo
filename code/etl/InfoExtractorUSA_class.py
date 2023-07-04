# Esta clase implementa los métodos de extracción de información mundial desde la API de USA

# Imports
import datetime as dt
import json
import InfoExtractor_class as iex
import requests
import pandas as pd

class InfoExtractorUSA(iex.InfoExtractor):
    '''
    Esta clase implementa los métodos solicitados en InfoExtractor.
    El objetivo de esta clase es el de extraer información desde la API de USA.
    '''

    def extractInfo(self, country: str, source: str, fromDateTime: dt.datetime, toDateTime: dt.datetime) -> (str, json):
        '''
        Este metodo permite extraer información de un país dado utilizando source como origen de los datos
        entre dos fechas dadas.
        El source solo puedo usarlo para validar que me están solicitando extraer información desde una fuente
        que conozco en este contexto.

        Ejemplo:

        extractInfo(country='US',
                    source='urlUSA',
                    fromDateTime=dt.datetime(2023,  7,  3,  0,  0,  0),
                    toDateTime=dt.datetime(2023,  7,  3, 23, 59, 59))

        Ejemplo salida:

        Tupla de(String de error, objeto Json conteniendo la info extraida)
        '''

        # Defino URL base para la consulta
        urlBase = 'https://earthquake.usgs.gov/fdsnws/event/1/query'

        # Defino variables para la consulta
        formato = 'geojson'
        fechaDesde = fromDateTime.strftime("%Y-%m-%d")
        fechaHasta = toDateTime.strftime("%Y-%m-%d")

        parametros = {'format' : formato,\
                    'starttime' : fechaDesde,\
                    'endtime' : fechaHasta}
        
        # Ejecuto el request
        response = requests.get(url=urlBase, params=parametros)

        # Obtengo los datos en formato json
        error = ''
        data = {}
        if response.status_code == 200:
            data = response.json()
        else:
            error = 'Response status difiere de 200'

        # TODO: Quedarme sólo con la info del país solicitado

        # Preparo la tupla de salida
        tupla_output = (error, data)

        # Retorno la tupla de salida
        return tupla_output