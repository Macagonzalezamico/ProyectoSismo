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
        japon = jsonData

        japon["Hora y dia del Sismo"] = japon["Hora y dia del Sismo"].astype(str)
        japon["Latitud"] = japon["Latitud"].astype(str)
        japon["Longitud"] = japon["Longitud"].astype(str)
        japon["Magnitud"] = japon["Magnitud"].astype(str)
        japon["Profundidad del Hipocentro"] = japon["Profundidad del Hipocentro"].astype(str)
        japon["Lugar del Epicentro"] = japon["Lugar del Epicentro"].astype(str)

        #Reemplazamos
        japon["Hora y dia del Sismo"] = japon["Hora y dia del Sismo"].str.replace("<td>", "").str.replace("</td>", "")
        japon["Latitud"] = japon["Latitud"].str.replace("</td>", "").str.replace("<td>", "")
        japon["Longitud"]= japon["Longitud"].str.replace("</td>", "").str.replace("<td>", "")
        japon["Magnitud"] = japon["Magnitud"].str.replace("</td>", "").str.replace("<td>", "")
        japon["Profundidad del Hipocentro"] = japon["Profundidad del Hipocentro"].str.replace("</td>", "").str.replace("<td>", "")
        japon["Lugar del Epicentro"] = japon["Lugar del Epicentro"].str.replace("</td>", "").str.replace("<td>", "")
        
        #reindex
        columnas = ["Fecha_del_sismo", "Hora_del_sismo", "Latitud", "Longitud", "Profundidad_Km", "Magnitud", "Tipo_Magnitud", "Lugar_del_Epicentro", "ID_Pais"]

        # #Reemplazamos
        japon["Latitud"] = japon["Latitud"].str.replace("N", "")
        japon["Latitud"] = japon["Latitud"].str.replace("S", "")
        japon["Longitud"] = japon["Longitud"].str.replace("E", "").str.replace("W", "")
        japon["Profundidad del Hipocentro"] = japon["Profundidad del Hipocentro"].str.replace("km","").str.replace("Poco profundo","1")


        # #Transformaciones a float
        japon["Latitud"] = japon["Latitud"].astype(float)
        japon["Longitud"] = japon["Longitud"].astype(float)
        japon["Magnitud"] = japon["Magnitud"].astype(float)
        japon["Hora y dia del Sismo"] = japon["Hora y dia del Sismo"].astype("datetime64[ns]")

        # #Insertamos Columnas
        japon["Hora_del_sismo"] = japon["Hora y dia del Sismo"]
        japon["Tipo_Magnitud"]="ML"
        japon["ID_Pais"] = "JP"

        # #Formato Fecha
        # df_2["Hora y dia del Sismo"] = pd.to_datetime(df_2["Hora y dia del Sismo"])
        # df_2["Hora_del_sismo"] = pd.to_datetime(df_2["Hora y dia del Sismo"]) #funcional


        #Hasta aqui funciona

        # # #Poblamos la columna de Hora
        # for indice, elemento in enumerate(df_2["Hora_del_sismo"]):
          #    df_2["Hora_del_sismo"][indice] = df_2["Hora_del_sismo"][indice].time()


        # # # #Solo Fecha
        # for indice, elemento in enumerate(df_2["Hora y dia del Sismo"]):
          #   df_2["Hora y dia del Sismo"][indice] = pd.to_datetime(df_2["Hora y dia del Sismo"][indice])
           #  df_2["Hora y dia del Sismo"][indice] = df_2["Hora y dia del Sismo"][indice].date().strftime("%Y-%m-%d")
              
        japon.rename(columns={"Hora y dia del Sismo": "Fecha_del_sismo", "Profundidad del Hipocentro": "Profundidad_Km", "Lugar del Epicentro": "Lugar_del_Epicentro"}, inplace=True)
        japon = japon.reindex(columns=columnas)
        
        return ("", japon)
