# Este archivo contiene la clase abstracta InfoFormatter

# Imports

import json
import pandas as pd
from abc import ABC, abstractmethod
from enum import Enum

class InfoFormatter(ABC):
    '''
    Un InfoFormatter es aquel modulo que toma la información en formato Json y produce una salida
    en formato estandarizado con la información extraída.
    Esta clase es una clase abstracta que plantea los mensajes que sus instancias deben implementar.
    '''

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def formatInfo(self, country: str, jsonData: json) -> (str, pd.DataFrame):
        '''
        Este metodo permite formatear información de un país dado utilizando jsonData como origen de los datos.
        country identifica el país al cual pertenecen los datos.

        Ejemplo:

        formatInfo(country='US',
                    jsonData=objetoJson)

        Ejemplo salida:

        Tupla de(String de error, objeto DataFrame conteniendo la info formateada según la estandarización)
        '''