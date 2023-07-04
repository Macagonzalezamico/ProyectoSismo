# Este archivo contiene la clase abstracta InfoExtractor

# Imports

import datetime as dt
import json
from abc import ABC, abstractmethod
from enum import Enum

class InfoExtractor(ABC):
    '''
    Un InfoExtractor es aquel modulo que toma la información desde una fuente externa y produce una salida
    en formato Json con la información extraída.
    Esta clase es una clase abstracta que plantea los mensajes que sus instancias deben implementar.
    '''

    @abstractmethod
    def extractInfo(self, country: str, source: str, fromDateTime: dt.datetime, toDateTime: dt.datetime) -> (str, json):
        '''
        Este metodo permite extraer información de un país dado utilizando source como origen de los datos
        entre dos fechas dadas.

        Ejemplo:

        extractInfo(country='US',
                    source='urlUSA',
                    fromDateTime=dt.datetime(2023,  7,  3,  0,  0,  0),
                    toDateTime=dt.datetime(2023,  7,  3, 23, 59, 59))

        Ejemplo salida:

        Tupla de(String de error, objeto Json conteniendo la info extraida)
        '''