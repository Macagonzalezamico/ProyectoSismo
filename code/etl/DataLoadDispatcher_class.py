# Este archivo contiene la clase DataLoadDispatcher

# Imports

import datetime as dt
from enum import Enum
from InfoExtractor_class import InfoExtractor
from InfoFormatter_class import InfoFormatter
from DBUpdater_class import DBUpdater
import logging

class DataLoadDispatcher():
    '''
    (Asignador de carga incremental).
    Esta clase tiene por objetivo servir como dispatcher de las tareas de ETL.
    '''

    def __init__(self, extractor_CL_dataset: InfoExtractor,
                       extractor_CL_url: InfoExtractor,
                       extractor_USA_api: InfoExtractor,
                       extractor_JP_url: InfoExtractor,
                       extractor_Damage_url: InfoExtractor,
                       formatter_CL: InfoFormatter,
                       formatter_USA: InfoFormatter,
                       formatter_JP: InfoFormatter,
                       formatter_Damage: InfoFormatter) -> None:
        
        self.extractor_CL_dataset = extractor_CL_dataset
        self.extractor_CL_url = extractor_CL_url
        self.extractor_USA_api = extractor_USA_api
        self.extractor_JP_url = extractor_JP_url
        self.extractor_Damage_url = extractor_Damage_url

        self.formatter_CL = formatter_CL
        self.formatter_USA = formatter_USA
        self.formatter_JP = formatter_JP
        self.formatter_Damage = formatter_Damage

        self.map_extractors = {'CL_datasetChile' : self.extractor_CL_dataset,
                               'CL_urlChile' : self.extractor_CL_url,
                               'US_urlUSA' : self.extractor_USA_api,
                               'JP_urlUSA' : self.extractor_USA_api,
                               'JP_urlJapon' : self.extractor_JP_url,
                               'urlDamage' : self.extractor_Damage_url}
        
        self.map_formatters = {'CL' : self.formatter_CL,
                               'US' : self.formatter_USA,
                               'JP' : self.formatter_JP,
                               'Damage' : self.formatter_Damage}

        self.context_d = {'countries' : [{'CL' : {'sources' : [('urlChile',
                                                                dt.datetime(2000,  1,  1,  0,  0,  0),
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
                        if sinceDateTime <= s_tuple[2] and\
                           upToDateTime >= s_tuple[1]:
                            
                            # La tupla está incluida parcial o totalmente en el periodo solicitado
                            fromDateTime = sinceDateTime
                            if s_tuple[1] > fromDateTime:
                                fromDateTime = s_tuple[1]
                            toDateTime = upToDateTime
                            if s_tuple[2] < toDateTime:
                                toDateTime = s_tuple[2]

                            # Hacer los cortes por la cantidad de días máxima. P.Ej. Cortes cada 30 días
                            diferencia = toDateTime - fromDateTime
                            diferencia_dias = diferencia.days
                            segmentoFromDateTime = fromDateTime
                            segmentoToDateTime = toDateTime
                            continuar_procesando = True

                            while continuar_procesando:

                                if diferencia_dias > s_tuple[3]:
                                    segmentoToDateTime = segmentoFromDateTime + dt.timedelta(days=s_tuple[3] - 1)
                                    segmentoToDateTime = segmentoToDateTime + dt.timedelta(hours=23, minutes=59, seconds=59)
                                    if segmentoToDateTime > toDateTime:
                                        segmentoToDateTime = toDateTime

                                    # Preparo y agrego el output
                                    output_tuple = (country, s_tuple[0], segmentoFromDateTime, segmentoToDateTime)
                                    countrySourcesAndDates.append(output_tuple)

                                    # Recalculo from y to
                                    segmentoFromDateTime = segmentoToDateTime + dt.timedelta(days=1, hours=-23, minutes=-59, seconds=-59) # Sumo un día al desde
                                    if segmentoFromDateTime > toDateTime:
                                        continuar_procesando = False
                                    else:
                                        diferencia = toDateTime - segmentoFromDateTime
                                        diferencia_dias = diferencia.days
                                else:

                                    # Preparo y agrego el output
                                    output_tuple = (country, s_tuple[0], segmentoFromDateTime, toDateTime)
                                    countrySourcesAndDates.append(output_tuple)
                                    continuar_procesando = False

        return countrySourcesAndDates

    def dispatch_workflow(self, country: str, country_and_source: str, parameters_tuple: tuple):
        '''
        Este método dispara la ejecución de los distintos módulos del workflow por cada país.
        '''

        # Informo el inicio de un workflow
        logging.info('Workflow para ' + country_and_source + ' despachado')

        # Dispatch módulo de extracción de datos
        logging.info('Modulo de extracción de datos despachado')
        extractor = self.map_extractors[country_and_source]
        error = None
        extracted_info = None
        if extractor is not None:
            error, extracted_info = extractor.extractInfo(country=parameters_tuple[0], source=parameters_tuple[1], fromDateTime=parameters_tuple[2], toDateTime=parameters_tuple[3])
            if error != '':
                logging.warning('Error en extracción de {}: '.format(country_and_source) + ' ' + error)
                return
            
        # Dispatch módulo de formateo de datos
        logging.info('Modulo de formateo de datos despachado')
        formatter = self.map_formatters[country]
        error = None
        formatted_info = None
        if formatter is not None:
            error, formatted_info = formatter.formatInfo(country=country, jsonData=extracted_info)
            if error != '':
                logging.warning('Error en formateo de {}: '.format(country) + ' ' + error)
                return
        
        # Dispatch database writter o file writter
        if formatted_info is not None:
            logging.info('Modulo de inserción de datos despachado')
            db_updater = DBUpdater()
            db_updater.update_sismos(country, formatted_info)
        
    def startLoadingPeriod(self, country: str, sinceDateTime: dt.datetime, upToDateTime: dt.datetime):
        '''
        Este método pone a trabajar al dispatcher desde una fecha (excluida) y hasta la fecha y hora informada.
        '''

        try:

            # Recupero las tuplas de parametros iniciando en la fecha informada más un día
            tuplas_parametros_ejec = self.getSourcesFor(sinceDateTime=sinceDateTime + dt.timedelta(days=1), upToDateTime=upToDateTime, country=country)

            for tupla_param in tuplas_parametros_ejec:

                # Las tuplas de parámetros contienen: (country, source, fromDateTime, toDateTime)
                country_and_source = tupla_param[0] + '_' + tupla_param[1]
                self.dispatch_workflow(country, country_and_source, tupla_param)

        except Exception as e:

            # Informo de una excepción ocurrida
            logging.warning('Ocurrió una excepción: ' + f'error {e}')

    def startLoadingSince(self, country: str, sinceDateTime: dt.datetime):
        '''
        Este método pone a trabajar al dispatcher desde una fecha (excluida) y hasta la fecha y hora en que
        se ejecuta este método.
        '''

        ahora = dt.datetime.now()
        self.startLoadingPeriod(country=country, sinceDateTime=sinceDateTime, upToDateTime=ahora)

if __name__ == '__main__':

    # Pruebo algunas funciones
    dispatcher = DataLoadDispatcher(None, None, None, None, None)

    # getSourcesFor
    sources = dispatcher.getSourcesFor(dt.datetime(2023,  6, 29,  0,  0,  0), dt.datetime(2023,  7,  2, 23, 59, 59), 'JP')
    print(sources)