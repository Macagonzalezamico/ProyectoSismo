# Este archivo contiene la clase abstracta InfoFormatter

# Imports

import pandas as pd
import InfoFormatter_class as ifc
import time
import datetime as dt

class InfoFormatterJapon(ifc.InfoFormatter):
    '''
    Un InfoFormatter es aquel modulo que toma la información en formato Json y produce una salida
    en formato estandarizado con la información extraída.
    Esta clase es una clase abstracta que plantea los mensajes que sus instancias deben implementar.
    '''

    def formatInfo(self, country: str, jsonData: pd.DataFrame):
        '''
        Este metodo permite formatear información de un país dado utilizando jsonData como origen de los datos.
        country identifica el país al cual pertenecen los datos.

        Ejemplo:

        formatInfo(country='US',
                    jsonData=objetoJson)

        Ejemplo salida:

        Tupla de(String de error, objeto DataFrame conteniendo la info formateada según la estandarización)
        '''
        df_2 = jsonData

        df_2["Hora y dia del Sismo"] = df_2["Hora y dia del Sismo"].astype(str)
        df_2["Latitud"] = df_2["Latitud"].astype(str)
        df_2["Longitud"] = df_2["Longitud"].astype(str)
        df_2["Magnitud"] = df_2["Magnitud"].astype(str)
        df_2["Profundidad del Hipocentro"] = df_2["Profundidad del Hipocentro"].astype(str)
        df_2["Lugar del Epicentro"] = df_2["Lugar del Epicentro"].astype(str)

        #Reemplazamos
        df_2["Hora y dia del Sismo"] = df_2["Hora y dia del Sismo"].str.replace("<td>", "").str.replace("</td>", "")
        df_2["Latitud"] = df_2["Latitud"].str.replace("</td>", "").str.replace("<td>", "")
        df_2["Longitud"]= df_2["Longitud"].str.replace("</td>", "").str.replace("<td>", "")
        df_2["Magnitud"] = df_2["Magnitud"].str.replace("</td>", "").str.replace("<td>", "")
        df_2["Profundidad del Hipocentro"] = df_2["Profundidad del Hipocentro"].str.replace("</td>", "").str.replace("<td>", "")
        df_2["Lugar del Epicentro"] = df_2["Lugar del Epicentro"].str.replace("</td>", "").str.replace("<td>", "")
        
        #reindex
        columnas = ["Fecha_del_sismo", "Hora_del_sismo", "Latitud", "Longitud", "Profundidad_Km", "Magnitud", "Tipo_Magnitud", "Lugar_del_Epicentro", "ID_Pais"]

        # #Reemplazamos
        df_2["Latitud"] = df_2["Latitud"].str.replace("N", "")
        df_2["Latitud"] = df_2["Latitud"].str.replace("S", "")
        df_2["Longitud"] = df_2["Longitud"].str.replace("E", "").str.replace("W", "")
        df_2["Profundidad del Hipocentro"] = df_2["Profundidad del Hipocentro"].str.replace("km","").str.replace("Poco profundo","1")


        # #Transformaciones a float
        df_2["Latitud"] = df_2["Latitud"].astype(float)
        df_2["Longitud"] = df_2["Longitud"].astype(float)
        df_2["Magnitud"] = df_2["Magnitud"].astype(float)

        # #Insertamos Columnas
        df_2["Hora_del_sismo"] = ""
        df_2["Tipo_Magnitud"]="ML"
        df_2["ID_Pais"] = "JP"

        # #Formato Fecha
        # df_2["Hora y dia del Sismo"] = pd.to_datetime(df_2["Hora y dia del Sismo"])
        df_2["Hora_del_sismo"] = pd.to_datetime(df_2["Hora y dia del Sismo"]) #funcional


        #Hasta aqui funciona

        # # #Poblamos la columna de Hora
        for indice, elemento in enumerate(df_2["Hora_del_sismo"]):
              df_2["Hora_del_sismo"][indice] = df_2["Hora_del_sismo"][indice].time()


        # # # #Solo Fecha
        for indice, elemento in enumerate(df_2["Hora y dia del Sismo"]):
             df_2["Hora y dia del Sismo"][indice] = pd.to_datetime(df_2["Hora y dia del Sismo"][indice])
             df_2["Hora y dia del Sismo"][indice] = df_2["Hora y dia del Sismo"][indice].date().strftime("%Y-%m-%d")
              
        df_2.rename(columns={"Hora y dia del Sismo": "Fecha_del_sismo", "Profundidad del Hipocentro": "Profundidad_Km", "Lugar del Epicentro": "Lugar_del_Epicentro"}, inplace=True)

        japon = df_2.copy()

        japon = japon.reindex(columns=columnas)
        return ("", japon)