# Este archivo contiene la clase abstracta InfoFormatter

# Imports

import pandas as pd
import InfoFormatter_class as ifc

class InfoFormatterChile(ifc.InfoFormatter):
    '''
    Un InfoFormatter es aquel modulo que toma la información en formato Json y produce una salida
    en formato estandarizado con la información extraída.
    Esta clase es una clase abstracta que plantea los mensajes que sus instancias deben implementar.
    '''

    def formatInfo(self, country: str, jsonData: any):
        '''
        Este metodo permite formatear información de un país dado utilizando jsonData como origen de los datos.
        country identifica el país al cual pertenecen los datos.

        Ejemplo:

        formatInfo(country='US',
                    jsonData=objetoJson)

        Ejemplo salida:

        Tupla de(String de error, objeto DataFrame conteniendo la info formateada según la estandarización)
        '''

        Chile = jsonData.copy()  # Hacer una copia del DataFrame para evitar modificaciones no deseadas

        # Para borrar las filas nulas:
        Chile = Chile.dropna(how='all')

        # Para desanidar las columnas 1, 3 y 5:
        Chile.loc[:, "Fecha local"] = Chile[1].str[:19]

        Chile.loc[:, "Lugar del epicentro"] = Chile[1].str[19:]

        Chile.loc[:, "Latitud"] = Chile[3].str[:7]

        Chile.loc[:, "Longitud"] = Chile[3].str[8:]

        Chile.loc[:, "Magnitud"] = Chile[5].str[:4]

        Chile.loc[:, "Tipo_Magnitud"] = Chile[5].str[4:]

        # Para borrar las columnas 1 2 y 3:
        Chile = Chile.drop([1,2,3], axis=1)

        # Para renombrar la columna 4:
        Chile = Chile.rename(columns={4: "Profundidad(Km)"})

        #Para ordenar los elementos por fecha:
        Chile["Fecha local"] = pd.to_datetime(Chile["Fecha local"])

        Chile = Chile.sort_values("Fecha local")

        # Para quitar la nomenclatura km de la columna Profundidad:
        Chile.loc[:, "Profundidad(Km)"] = Chile["Profundidad(Km)"].str.replace("km", "")

        # Para resetear el índice de las filas:
        Chile = Chile.reset_index(drop=True)

        # Para separar las fechas de los horarios:
        Chile.loc[:, "Fecha del sismo"] = pd.to_datetime(Chile["Fecha local"]).dt.date
        Chile.loc[:, "Hora del sismo"] = pd.to_datetime(Chile["Fecha local"]).dt.time

        #Para eliminar la columna fecha local:
        Chile.drop(["Fecha local"], axis=1)

        #Para crear una columna con el ID del país:
        Chile["ID_País"] = "CL"

        # Para transformar Profundidad a float:
        Chile.loc[:, "Profundidad(Km)"] = Chile["Profundidad(Km)"].astype(float)

        #Para reordenar las columnas:
        columnas = ["Fecha del sismo", "Hora del sismo", "Latitud", "Longitud", "Profundidad(Km)", "Magnitud", "Tipo_Magnitud", "Lugar del epicentro", "ID_País"]

        Chile = Chile.reindex(columns=columnas)

        return ("", Chile)
