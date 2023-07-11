# Este archivo contiene la clase DBUpdater

# Imports
import pandas as pd
import datetime as dt
from google.cloud import bigquery
from google.oauth2 import service_account
from ETLEnvironment_class import ETLEnvironment

class DBUpdater():
    '''
    El DBUpdater se ocupa de actualizar la base de datos Big Query
    con la información extraída y formateada dentro del pipeline
    '''

    def __init__(self) -> None:
        pass

    def get_google_cloud_client(self):
        '''
        Esta función devuelve un cliente de Google Cloud listo para ser utilizado en el proyecto sismos
        '''
        print('get_google_cloud_client')

        # Preparo el path y scope para recuperar las credenciales
        path_root = ETLEnvironment().root_project_path
        json_credentials = "ProyectoSismo/data/project-sismos-f5e5a5846eab.json"
        first_scope = "https://www.googleapis.com/auth/cloud-platform"
        credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

        return client

    def update_last_dates(self, ult_fecha_japon: dt.date=None, ult_fecha_chile: dt.date=None, ult_fecha_usa: dt.date=None):
        '''
        Esta función actualiza la base de datos con la fecha o las fechas que se le ha informado
        '''
        print('update_last_dates')

        client = self.get_google_cloud_client()

        # Japón
        if ult_fecha_japon is not None:

            query = '''
                    UPDATE sismos_db.fechas
                    SET ultima_fecha_japon = DATE(''' + ult_fecha_japon.strftime("%Y,%m,%d") + ''')
                    WHERE 1 = 1;
                    '''
            
            job = client.query(query)
            result = job.result()

        # Chile
        if ult_fecha_chile is not None:

            query = '''
                    UPDATE sismos_db.fechas
                    SET ultima_fecha_chile = DATE(''' + ult_fecha_chile.strftime("%Y,%m,%d") + ''')
                    WHERE 1 = 1;
                    '''
            
            job = client.query(query)
            result = job.result()

        # USA
        if ult_fecha_usa is not None:

            query = '''
                    UPDATE sismos_db.fechas
                    SET ultima_fecha_usa = DATE(''' + ult_fecha_usa.strftime("%Y,%m,%d") + ''')
                    WHERE 1 = 1;
                    '''
            
            job = client.query(query)
            result = job.result()

    def get_query_row(self, row):
        '''
        Esta función devuelve un string que se corresponde con una fila para el insert de datos en la tabla sismos.
        '''
        print(' get_query_row')
        print(' Fecha_del_sismo', row['Fecha_del_sismo'])
        print(' Hora_del_sismo', row['Hora_del_sismo'])
        #print(' Latitud', row['Latitud'])
        #print(' Longitud', row['Longitud'])
        #print(' Profundidad_Km', row['Profundidad_Km'])
        #print(' Magnitud', row['Magnitud'])
        #print(' Tipo_Magnitud', row['Tipo_Magnitud'])
        #print(' Lugar_del_Epicentro', row['Lugar_del_Epicentro'])
        print(' ID_Pais', row['ID_Pais'])

        query_row = '(' +\
                        'DATE(' +\
                        str(row['Fecha_del_sismo'].year) + ',' +\
                        str(row['Fecha_del_sismo'].month) + ',' +\
                        str(row['Fecha_del_sismo'].day) +\
                        '), ' +\
                        'TIME(' +\
                        str(row['Hora_del_sismo'].hour) + ',' +\
                        str(row['Hora_del_sismo'].minute) + ',' +\
                        str(row['Hora_del_sismo'].second) +\
                        '), ' +\
                        str(row['Latitud']) +\
                        ', ' +\
                        str(row['Longitud']) +\
                        ', ' +\
                        str(row['Profundidad_Km']) +\
                        ', ' +\
                        str(row['Magnitud']) +\
                        ', ' +\
                        '\'' + row['Tipo_Magnitud'] + '\'' +\
                        ', ' +\
                        '\'' + ('' if row['Lugar_del_Epicentro'] is None else row['Lugar_del_Epicentro']) + '\'' +\
                        ', ' +\
                        '\'' + row['ID_Pais'] + '\'' +\
                    ')'
        
        return query_row
    
    def insert_rows(self, insert_query: str):
        '''
        Esta función inserta los registros en la base de datos en la tabla sismos.
        '''
        print('insert_rows')

        client = self.get_google_cloud_client()

        # Ejecuta la inserción
        job = client.query(insert_query)
        result = job.result()

    def update_sismos(self, country: str, data: pd.DataFrame):
        '''
        Esta función actualiza la base de datos Big Query con el dataframe recibido.
        Además almacena la última fecha que aparece en los registros por país para que
        sirva como input a futuras cargas
        '''
        print('update_sismos')
        
        # Verifico si las columnas obtenidas son las esperadas
        expected_columns = ['Fecha_del_sismo', 'Hora_del_sismo', 'Latitud', 'Longitud', 'Profundidad_Km', 'Magnitud', 'Tipo_Magnitud', 'Lugar_del_Epicentro', 'ID_Pais']
        data_columns = data.columns
        columns_ok = True
        for c in expected_columns:
            if c not in data_columns:
                columns_ok = False

        # Si las columnas obtenidas son las esperadas, procedo con
        # la carga en Big Query
        if columns_ok:

            # Defino una last date que luego utilizaré para actualizar la última fecha de actualización
            # de ese país en Big Query
            last_date = data['Fecha_del_sismo'].max()
            
            # Defino la query de inserción de datos
            query_begin = 'INSERT INTO `sismos_db.sismos` (`Fecha_del_sismo`, `Hora_del_sismo`, `Latitud`, `Longitud`, `Profundidad_Km`, `Magnitud`, `Tipo_Magnitud`, `Lugar_del_Epicentro`, `ID_Pais`) VALUES '
            query_body = ''

            intercalar_coma = False
            for idx, row in data.iterrows():
                if intercalar_coma:
                    query_body = query_body + ', '
                else:
                    intercalar_coma = True
                query_row = self.get_query_row(row)
                query_body = query_body + query_row

            query_end = ';'

            insert_query = query_begin + query_body + query_end

            self.insert_rows(insert_query)

            # Actualizo la ultima fecha del país que se trate
            if country == 'JP':
                self.update_last_dates(ult_fecha_japon=last_date)
            elif country == 'CL':
                self.update_last_dates(ult_fecha_chile=last_date)
            else:
                self.update_last_dates(ult_fecha_usa=last_date)

        else:

            print('Los nombres de las columnas en el dataFrame difieren de lo esperado')
            print(' Las columnas esperadas son:')
            print(expected_columns)
