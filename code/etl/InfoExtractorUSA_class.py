# Esta clase implementa los métodos de extracción de información mundial desde la API de USA

# Imports
import datetime as dt
import json
from InfoExtractor_class import InfoExtractor
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from ETLEnvironment_class import ETLEnvironment

# Para probar esta clase, puede utilizar el siguiente código
# a = InfoExtractorUSA()
# a.extractInfo('US', 'urlUSA', dt.datetime(2023,  7,  3,  0,  0,  0), dt.datetime(2023,  7,  4, 23, 59, 59))

# Información de polígonos de países extraida desde
# https://datahub.io/core/geo-countries#pandas
# La misma se almacenó en el directorio ./data/countries.geojson

class InfoExtractorUSA(InfoExtractor):
    '''
    Esta clase implementa los métodos solicitados en InfoExtractor.
    El objetivo de esta clase es el de extraer información desde la API de USA.
    '''

    def __init__(self) -> None:
        super().__init__()

    def get_country_boundaries(self, iso_a3_country_code: str):
        '''
        Este método recupera la información relacionada con el país solicitado.
        Incluye geometry que puede contener POLYGON o MULTIPOLYGON
        '''
        print(' get_country_boundaries')

        # Obtengo los mapas
        env = ETLEnvironment().root_project_path
        countries_boundaries = gpd.read_file(env + 'ProyectoSismo/data/countries.geojson')

        # Obtengo la info del país desde el dataframe
        country_data = countries_boundaries[countries_boundaries['ISO_A3'] == iso_a3_country_code]
        country_data.reset_index(inplace=True, drop=True)
        country_boundaries = country_data.loc[0]

        # Devuelvo la info del país solicitado
        return country_boundaries
    
    def get_seismic_records(self, format: str, fromDateTime: dt.datetime, toDateTime: dt.datetime):
        '''
        Este método devuelve los registros sismológicos entre dos fechas dadas.
        No diferencia entre un país y otro
        '''
        print(' get_seismic_records')

        # Defino URL base para la consulta
        urlBase = 'https://earthquake.usgs.gov/fdsnws/event/1/query'

        # Defino variables para la consulta
        formato = format
        fechaDesde = fromDateTime
        fechaHasta = toDateTime

        parametros = {'format' : formato,\
                    'starttime' : fechaDesde,\
                    'endtime' : fechaHasta}
        
        # Ejecuto el request
        response = requests.get(url=urlBase, params=parametros)

        # Obtengo los datos en formato json
        error = ''
        data = {}
        if response.status_code == 200:
            data = response.json()
        else:
            error = 'Response status difiere de 200'

        # Preparo la tupla de salida
        tupla_output = (error, data)

        # Retorno la tupla de salida
        return tupla_output
    
    def convert_date(self, fecha: int):
        '''
        Esta función convierte la fecha según lo informa la API a un formato iso.
        '''
        print(' convert_date', fecha, type(fecha))
        
        fecha = fecha / 1000 # Elimino los milisegundos
        fechaFormateada = dt.datetime.isoformat(dt.datetime.fromtimestamp(fecha))
        return fechaFormateada

    def get_dataframe_from_json(self, data: dict):
        '''
        Dado un GeoJson con registros sismológicos, este método devuelve un dataframe.
        '''
        print(' get_dataframe_from_json')

        # Preparo un dataframe para incorporar la info
        usa_df = pd.DataFrame()

        # Recorro el diccionario para ir extrayendo la info
        fechaGeneracion = data['metadata']['generated']
        titulo = data['metadata']['title']
        versionAPI = data['metadata']['api']
        cantRegistros = data['metadata']['count']

        # Extraigo info de los sismos
        # Recorro los registros para des-anidar los datos
        for registro in data['features']:

            tipoReg = registro['type']
            idReg = registro['id']

            # Desanido properties
            properties_df = pd.DataFrame(registro['properties'], index=[0, 1, 2])
            properties_df.drop_duplicates(inplace=True, ignore_index=True)

            # Desanido geometry
            pointCoord = {}
            if registro['geometry']['type'] == 'Point':
                pointCoord['type'] = 'Point'
                pointCoord['coord1'] = registro['geometry']['coordinates'][0]
                pointCoord['coord2'] = registro['geometry']['coordinates'][1]
                pointCoord['coord3'] = registro['geometry']['coordinates'][2]

                # Agrego las coordenadas si es que la geometria es un Point
                properties_df['geometry_type'] = pointCoord['type']
                properties_df['geometry_coord1'] = pointCoord['coord1']
                properties_df['geometry_coord2'] = pointCoord['coord2']
                properties_df['geometry_coord3'] = pointCoord['coord3']

            # Incorporo info del dataset
            properties_df['fechaGeneracion'] = fechaGeneracion
            properties_df['titulo'] = titulo
            properties_df['versionAPI'] = versionAPI
            properties_df['cantRegistros'] = cantRegistros

            # Agrego la info al dataframe de USA
            usa_df = pd.concat([usa_df, properties_df], ignore_index=True)

        # Quito los milisegundos y modifico tipos de datos
        usa_df['fechaGeneracion'] = usa_df['fechaGeneracion'].apply(self.convert_date)
        usa_df['time'] = usa_df['time'].apply(self.convert_date)
        usa_df['updated'] = usa_df['updated'].apply(self.convert_date)
        print(' Fuera de convert_date')

        #usa_df['fechaGeneracion'] = usa_df['fechaGeneracion'].astype('datetime64[us]')
        usa_df['time'] = usa_df['time'].astype('datetime64[us]')
        #usa_df['updated'] = usa_df['updated'].astype('datetime64[us]')
        print(' Luego de hacer astype("datetime64[us]")')

        # Devuelvo el dataframe
        return usa_df
    
    def keep_only_records_for_boundaries(self, data:pd.DataFrame, country_boundaries: any):
        '''
        Este método devuelve el dataframe recibido donde el Point queda dentro de las boundaries
        '''
        print(' keep_only_records_for_boundaries')

        data['keep_record'] = 'no'
        for idx, row in data.iterrows():
            if row['geometry_type'] == 'Point':
                given_point = Point(row['geometry_coord1'], row['geometry_coord2'])
                is_inside = given_point.within(country_boundaries.geometry)
                if is_inside:
                    data.at[idx, 'keep_record'] = 'yes'
        
        data = data[data['keep_record'] == 'yes']
        data = data.drop(columns='keep_record')

        return data

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
        print(' extractInfo')

        # Obtengo el mapa del país que correspnda
        iso_a3_country_code = 'USA'
        if country == 'JP':
            iso_a3_country_code = 'JPN'
        elif country == 'CL':
            iso_a3_country_code = 'CHL'
        country_boundaries = self.get_country_boundaries(iso_a3_country_code)

        # Obtengo en formato GeoJson los registros de sismos entre las fechas solicitadas
        error, data = self.get_seismic_records(format='geojson', fromDateTime=fromDateTime.strftime("%Y-%m-%d"), toDateTime=toDateTime.strftime("%Y-%m-%d"))

        # Transformo lo obtenido en un dataframe
        seismic_df = self.get_dataframe_from_json(data=data)

        # Me quedo sólo con la info del país solicitado
        seismic_df = self.keep_only_records_for_boundaries(data=seismic_df, country_boundaries=country_boundaries)

        # Preparo la tupla de salida
        tupla_output = (error, seismic_df)

        # Retorno la tupla de salida
        return tupla_output
