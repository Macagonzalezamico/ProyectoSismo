# Esta clase implementa los métodos de extracción de información de Chile desde la URL de Chile

# Imports
import datetime as dt
import json
import InfoExtractor_class as iex
import requests
import pandas as pd

class InfoExtractorChileURL(iex.InfoExtractor):
    '''
    Esta clase implementa los métodos solicitados en InfoExtractor.
    El objetivo de esta clase es el de extraer información desde la URL de Chile.
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

