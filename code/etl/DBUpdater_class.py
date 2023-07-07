# Este archivo contiene la clase DBUpdater

# Imports
import pandas as pd
import datetime as dt
from google.cloud import bigquery
from google.oauth2 import service_account

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

        # IMPORTANTE! MODIFICAR path_root EN PRODUCCION
        path_root = "/Users/fernandoembrioni/Documents/Fer/Capacitacion/soyhenry/data_science/repos/50_Proyectos/PGrupal/repo/"
        json_credentials = "ProyectoSismo/data/project-sismos-f5e5a5846eab.json"
        first_scope = "https://www.googleapis.com/auth/cloud-platform"
        credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

        return client

    def set_last_dates(self, ult_fecha_japon: dt.date=None, ult_fecha_chile: dt.date=None, ult_fecha_usa: dt.date=None):
        '''
        Esta función actualiza la base de datos con la fecha o las fechas que se le ha informado
        '''

        client = self.get_google_cloud_client()

        # Japón
        if ult_fecha_japon is not None:

            query = '''
                    UPDATE sismos_db.fechas
                    SET ultima_fecha_japon = DATE(''' + ult_fecha_japon.strftime("%Y,%m,%d") + ''')
                        sismos_db.fechas
                    WHERE 1 = 1;
                    '''
            
            job = client.query(query)
            result = job.result()

        # Chile
        if ult_fecha_chile is not None:

            query = '''
                    UPDATE sismos_db.fechas
                    SET ultima_fecha_chile = DATE(''' + ult_fecha_chile.strftime("%Y,%m,%d") + ''')
                        sismos_db.fechas
                    WHERE 1 = 1;
                    '''
            
            job = client.query(query)
            result = job.result()

        # USA
        if ult_fecha_usa is not None:

            query = '''
                    UPDATE sismos_db.fechas
                    SET ultima_fecha_usa = DATE(''' + ult_fecha_usa.strftime("%Y,%m,%d") + ''')
                        sismos_db.fechas
                    WHERE 1 = 1;
                    '''
            
            job = client.query(query)
            result = job.result()

    def update_sismos(self, country: str, data: pd.DataFrame):
        '''
        Esta función actualiza la base de datos Big Query con el dataframe recibido.
        Además almacena la última fecha que aparece en los registros por país para que
        sirva como input a futuras cargas
        '''
        
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
            last_date = dt.date(2000,1,1)
            # TODO: Carga en bigquery
            print('Pendiente hacer la carga de los sismos en Big Query')
        else:
            print('Los nombres de las columnas en el dataFrame difieren de lo esperado')
            print(' Las columnas esperadas son:')
            print(expected_columns)
