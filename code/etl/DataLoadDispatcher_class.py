# Este archivo contiene la clase DataLoadDispatcher

# Imports

import datetime as dt
from enum import Enum
import InfoExtractor_class as iex
#import extractor_CL_dataset class
#import extractor_CL_url class
#import extractor_USA_api class
#import extractor_JP_url class
#import extractor_Damage_url class

class DataLoadDispatcher():
    '''
    (Asignador de carga incremental).
    Esta clase tiene por objetivo servir como dispatcher de las tareas de ETL.
    '''

    def __init__(self, extractor_CL_dataset: iex.InfoExtractor,
                       extractor_CL_url: iex.InfoExtractor,
                       extractor_USA_api: iex.InfoExtractor,
                       extractor_JP_url: iex.InfoExtractor,
                       extractor_Damage_url: iex.InfoExtractor) -> None:
        
        self.extractor_CL_dataset = extractor_CL_dataset
        self.extractor_CL_url = extractor_CL_url
        self.extractor_USA_api = extractor_USA_api
        self.extractor_JP_url = extractor_JP_url
        self.extractor_Damage_url = extractor_Damage_url

        self.map_extractors = {'CL_datasetChile' : self.extractor_CL_dataset,
                               'CL_urlChile' : self.extractor_CL_url,
                               'US_urlUSA' : self.extractor_USA_api,
                               'JP_urlUSA' : self.extractor_USA_api,
                               'JP_urlJapon' : self.extractor_JP_url,
                               'urlDamage' : self.extractor_Damage_url}

        self.context_d = {'countries' : [{'CL' : {'sources' : [('datasetChile', # Fuente de la información
                                                                dt.datetime(2000,  1,  1,  0,  0,  0), # Fecha desde validez de esta fuente
                                                                dt.datetime(2015,  8, 19, 23, 59, 59), # Fecha hasta validez de esta fuente
                                                                30), # Cantidad máxima de días a extraer por lote
                                                               ('urlChile',
                                                                dt.datetime(2015,  8, 20,  0,  0,  0),
                                                                dt.datetime(9999, 12, 31, 23, 59, 59),
                                                                30)]}},
                                         {'US' : {'sources' : [('urlUSA',
                                                                dt.datetime(2000,  1,  1,  0,  0,  0),
                                                                dt.datetime(9999, 12, 31, 23, 59, 59),
                                                                30)]}},
                                         {'JP' : {'sources' : [('urlUSA',
                                                                dt.datetime(2000,  1,  1,  0,  0,  0),
                                                                dt.datetime(2023,  6, 30, 23, 59, 59),
                                                                30),
                                                               ('urlJapon',
                                                                dt.datetime(2023,  7,  1,  0,  0,  0),
                                                                dt.datetime(9999, 12, 31, 23, 59, 59),
                                                                30)]}}],
                         'damage' : ('urlDamage',
                                     dt.datetime(1800,  1,  1,  0,  0,  0),
                                     dt.datetime(9999, 12, 31, 23, 59, 59),
                                     30)}
        
    def getSourcesFor(self, sinceDateTime: dt.datetime, upToDateTime: dt.datetime, country: str):
        '''
        Este metodo devuelve los nombres de los recursos a utilizar según las fechas del periodo solicitado,
        tomando en cuenta la cantidad de días máximo que se puede solicitar por vez, por lo que para un mismo
        país podrían recibirse varios registros.

        Ejemplo de llamada:

        getSourcesFor(sinceDateTime=dt.datetime(2023,  6, 29,  0,  0,  0),
                      upToDateTime=dt.datetime(2023,  7,  2, 23, 59, 59),
                      country='JP')

        Ejemplo de salida:

        [('JP', 'urlUSA', dt.datetime(2023,  6, 29,  0,  0,  0), dt.datetime(2023,  6, 30, 23, 59, 59)),
         ('JP', 'urlJapon', dt.datetime(2023,  7,  1,  0,  0,  0), dt.datetime(2023,  7,  2, 23, 59, 59))]
        '''

        countrySourcesAndDates = []

        for d_countries in self.context_d['countries']:
            for c in d_countries:
                if c == country:
                    for s_tuple in d_countries[c]['sources']:
                        print(s_tuple)
                        if sinceDateTime <= s_tuple[2] and\
                           upToDateTime >= s_tuple[1]:
                            
                            # La tupla está incluida parcial o totalmente en el periodo solicitado
                            fromDateTime = sinceDateTime
                            if s_tuple[1] > fromDateTime:
                                fromDateTime = s_tuple[1]
                            toDateTime = upToDateTime
                            if s_tuple[2] < toDateTime:
                                toDateTime = s_tuple[2]

                            # TODO: Hacer los cortes por la cantidad de días máxima. P.Ej. Cortes cada 30 días

                            # Preparo y agrego el output
                            output_tuple = (country, s_tuple[0], fromDateTime, toDateTime)
                            countrySourcesAndDates.append(output_tuple)

        return countrySourcesAndDates

    def startLoadingPeriod(self, sinceDateTime: dt.datetime, upToDateTime: dt.datetime):
        '''
        Este método pone a trabajar al dispatcher desde una fecha (excluida) y hasta la fecha y hora informada.
        '''

        # TODO: Implementar este método.
        # Considerar que otro proceso podría estar en curso

    def startLoadingSince(self, sinceDateTime: dt.datetime):
        '''
        Este método pone a trabajar al dispatcher desde una fecha (excluida) y hasta la fecha y hora en que
        se ejecuta este método.
        '''

        ahora = dt.datetime.now()
        self.startLoadingPeriod(sinceDateTime=sinceDateTime, upToDateTime=ahora)

if __name__ == '__main__':

    # Pruebo algunas funciones
    dispatcher = DataLoadDispatcher(None, None, None, None, None)

    # getSourcesFor
    sources = dispatcher.getSourcesFor(dt.datetime(2023,  6, 29,  0,  0,  0), dt.datetime(2023,  7,  2, 23, 59, 59), 'JP')
    print(sources)