# Este archivo contiene la clase abstracta InfoFormatter

# Imports

import json
import pandas as pd
from abc import ABC, abstractmethod

class InfoFormatterChile(ABC):
    '''
    Un InfoFormatter es aquel modulo que toma la información en formato Json y produce una salida
    en formato estandarizado con la información extraída.
    Esta clase es una clase abstracta que plantea los mensajes que sus instancias deben implementar.
    '''

    @abstractmethod
    def formatInfoChile(self, DataFrame):
        '''
        Este metodo permite formatear información de un país dado utilizando jsonData como origen de los datos.
        country identifica el país al cual pertenecen los datos.

        Ejemplo:

        formatInfo(country='US',
                    jsonData=objetoJson)

        Ejemplo salida:

        Tupla de(String de error, objeto DataFrame conteniendo la info formateada según la estandarización)
        '''

        Chile = DataFrame.copy()  # Hacer una copia del DataFrame para evitar modificaciones no deseadas

        # Para borrar las filas nulas:
        Chile = Chile.dropna(how='all')

        # Para desanidar las columnas 1 y 3:
        Chile.loc[:, "Fecha local"] = Chile[1].str[:19]

        Chile.loc[:, "Referencia"] = Chile[1].str[19:]

        Chile.loc[:, "Latitud"] = Chile[3].str[:7]

        Chile.loc[:, "Longitud"] = Chile[3].str[8:]

        Chile.loc[:, "Magnitud"] = Chile[5].str[:4]

        Chile.loc[:, "TipoDeMagnitud"] = Chile[5].str[4:]

        # Para borrar las columnas 1 y 3:
        Chile = Chile.drop([1, 3], axis=1)

        # Para renombrar las columnas 2, 4 y 5:
        Chile = Chile.rename(columns={2: "Fecha UTC", 4: "Profundidad(Km)"})

        # Para quitar la nomenclatura km de la columna Profundidad:
        Chile.loc[:, "Profundidad(Km)"] = Chile["Profundidad(Km)"].str.replace("km", "")

        # Para resetear el índice de las filas:
        Chile = Chile.reset_index(drop=True)

        # Para normalizar las columnas Datetime:
        Chile.loc[:, "Fecha local"] = pd.to_datetime(Chile["Fecha local"])

        Chile.loc[:, "Fecha UTC"] = pd.to_datetime(Chile["Fecha UTC"])

        # Para transformar Profundidad a float:
        Chile.loc[:, "Profundidad(Km)"] = Chile["Profundidad(Km)"].astype(float)

        columnas = ["Fecha local", "Fecha UTC", "Latitud", "Longitud", "Profundidad(Km)", "Magnitud", "TipoDeMagnitud", "Referencia"]

        Chile = Chile.reindex(columns=columnas)

        return Chile
