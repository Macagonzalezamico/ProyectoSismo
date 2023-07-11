# Este archivo contiene la clase InfoFormatterUSA

# Imports

from InfoFormatter_class import InfoFormatter
import pandas as pd
import datetime as dt

class InfoFormatterUSA(InfoFormatter):
    '''
    Este InfoFormatter para USA recibe un dataFrame y convierte el mismo a un dataframe según el formato
    esperado por el DBUpdater.
    '''

    def __init__(self) -> None:
        super().__init__()

    def formatInfo(self, country: str, jsonData: pd.DataFrame) -> (str, pd.DataFrame):
        '''
        Este metodo permite formatear información de un país dado utilizando un dataFrame como origen de los datos.
        country identifica el país al cual pertenecen los datos.

        Ejemplo:

        formatInfo(country='US',
                    jsonData=objetoJson)

        Ejemplo salida:

        Tupla de(String de error, objeto DataFrame conteniendo la info formateada según la estandarización)
        '''

        print('formatInfo', country)

        # Identifico mi dataFrame de entrada como df
        df = jsonData

        # Elimino duplicados si los ubiera y en los casos de valores nulos en campos de interés,
        # elimino el registro o imputo los datos
        df.drop_duplicates(inplace=True)
        df.dropna(subset=['mag', 'place', 'time', 'geometry_coord1', 'geometry_coord2', 'geometry_coord3', 'magType'])

        # Renombro los campos útiles
        df = df.rename(columns={'mag' : 'Magnitud',\
                                'place' : 'Lugar_del_Epicentro',\
                                'geometry_coord1' : 'Longitud',\
                                'geometry_coord2' : 'Latitud',\
                                'geometry_coord3' : 'Profundidad_Km',\
                                'magType' : 'Tipo_Magnitud'})
        
        # Genero los campos de fecha y hora
        df['Fecha_del_sismo'] = df['time'].apply(lambda fecha_y_hora: dt.date(fecha_y_hora.year, fecha_y_hora.month, fecha_y_hora.day))
        df['Hora_del_sismo'] = df['time'].apply(lambda fecha_y_hora: dt.time(fecha_y_hora.hour, fecha_y_hora.minute, fecha_y_hora.second))

        # Reordeno las columnas
        df = df[['Fecha_del_sismo', 'Hora_del_sismo', 'Latitud', 'Longitud', 'Profundidad_Km', 'Magnitud', 'Tipo_Magnitud', 'Lugar_del_Epicentro']]

        # Agrego la columna de ID del país
        df = df.assign(ID_Pais=country)

        print(df.head())

        # Devuelvo el dataFrame con mensaje de error vacío
        return ('', df)