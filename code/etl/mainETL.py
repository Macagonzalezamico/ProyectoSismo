# Este es el inicio del proceso de ETL
# El mismo se comunica con BigQuery para obtener el contexto de ejecución (Flags y fechas)

# Imports
from DataLoadDispatcher_class import DataLoadDispatcher
from InfoExtractorChileURL_class import InfoExtractorChileURL
from InfoFormatterChile_class import InfoFormatterChile
from InfoExtractorUSA_class import InfoExtractorUSA
from InfoFormatterUSA_class import InfoFormatterUSA
from InfoExtractorJaponURL_ClassD import InfoExtractorJaponURL
from InfoFormatterJapon_ClassD import InfoFormatterJapon
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime as dt
from ETLEnvironment_class import ETLEnvironment
import logging

def get_google_cloud_client():
    '''
    Esta función devuelve un cliente de Google Cloud listo para ser utilizado en el proyecto sismos
    '''

    # Preparo el path y scope para recuperar las credenciales
    path_root = ETLEnvironment().root_project_path
    json_credentials = "ProyectoSismo/data/project-sismos-f5e5a5846eab.json"
    first_scope = "https://www.googleapis.com/auth/cloud-platform"
    credentials = service_account.Credentials.from_service_account_file(path_root + json_credentials, scopes=[first_scope],)

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    return client

def get_flag_process_in_progress():
    '''
    Esta función obtiene el flag de proceso en progreso desde la base de datos
    '''

    flag_processInProgress = True

    client = get_google_cloud_client()
    query = '''
            SELECT
                flags.flag_dataloadinginprogress
            FROM
                sismos_db.flags
            LIMIT 1;
            '''
    
    job = client.query(query)

    for row in job.result():
        flag_processInProgress = row[0]

    return flag_processInProgress
    
def set_flag_process_in_progress(value: bool):
    '''
    Esta función setea el flag de proceso en progreso desde la base de datos
    '''

    flag_processInProgress = 'false'
    if value:
        flag_processInProgress = 'true'

    client = get_google_cloud_client()
    query = '''
            UPDATE
                sismos_db.flags
            SET flag_dataloadinginprogress = ''' + flag_processInProgress + '''
            WHERE 1 = 1;
            '''
    
    job = client.query(query)

    return job.result()

def get_last_dates():
    '''
    Esta función busca en la base de datos las últimas fechas de carga de cada país.
    Retorna ult_fecha_japon, ult_fecha_chile, ult_fecha_usa en una tupla
    '''

    client = get_google_cloud_client()
    query = '''
            SELECT *
            FROM
                sismos_db.fechas
            LIMIT 1;
            '''
    
    job = client.query(query)

    ult_fecha_japon = dt.datetime.now() - dt.timedelta(days=7)
    ult_fecha_chile = dt.datetime.now() - dt.timedelta(days=7)
    ult_fecha_usa = dt.datetime.now() - dt.timedelta(days=7)

    for row in job.result():
        date_japon = row['ultima_fecha_japon']
        date_chile = row['ultima_fecha_chile']
        date_usa = row['ultima_fecha_usa']

        logging.debug('En get_last_dates')
        logging.debug(' date_japon ' + str(date_japon))
        logging.debug(' date_chile ' + str(date_chile))
        logging.debug(' date_usa ' + str(date_usa))

        ult_fecha_japon = dt.datetime(date_japon.year, date_japon.month, date_japon.day, 0, 0, 0)
        ult_fecha_chile = dt.datetime(date_chile.year, date_chile.month, date_chile.day, 0, 0, 0)
        ult_fecha_usa = dt.datetime(date_usa.year, date_usa.month, date_usa.day, 0, 0, 0)

    return (ult_fecha_japon, ult_fecha_chile, ult_fecha_usa)

def get_dispatcher() -> DataLoadDispatcher:
    '''
    Esta función devuelve un dispatcher configurado con los distintos extractors y formatters.
    '''

    # Instancio los módulos
    ieUSA = InfoExtractorUSA()
    ifUSA = InfoFormatterUSA()
    ieChile = InfoExtractorChileURL()
    ifChile = InfoFormatterChile()
    ieJapon = InfoExtractorJaponURL()
    ifJapon = InfoFormatterJapon()

    d = DataLoadDispatcher(extractor_CL_dataset=None,\
                           extractor_CL_url=ieChile,\
                           extractor_JP_url=ieJapon,\
                           extractor_USA_api=ieUSA,\
                           extractor_Damage_url=None,\
                           formatter_CL=ifChile,\
                           formatter_JP=ifJapon,\
                           formatter_USA=ifUSA,\
                           formatter_Damage=None)
    
    # Retorno el dispatcher
    return d

if __name__ == '__main__':

    # Seteo el logging
    logging.basicConfig(filename="logETL.txt", level=logging.INFO) # Este es el nivel usual de debugging
    #logging.basicConfig(filename="logETL.txt", level=logging.DEBUG)

    # Logeo la fecha de procesamiento
    logging.info('.')
    logging.info('----------------------------------------------')
    logging.info('Proceso iniciado el ' + str(dt.datetime.now()))

    # Obtengo el flag de proceso en curso
    process_in_progress = get_flag_process_in_progress()
    if process_in_progress:
        logging.info('Este proceso no puede ejecutarse porque ya hay un proceso de carga en progreso')
    else:

        # Se inicia el proceso
        logging.info('Proceso de ETL iniciado')

        # Se pone el flag de proceso en progreso
        set_flag_process_in_progress(True)

        # Se obtienen las ultimas fechas de proceso desde la base de datos
        ult_fecha_japon, ult_fecha_chile, ult_fecha_usa = get_last_dates()

        # Se invoca el dispatcher por cada país y última fecha de carga
        dispatcher = get_dispatcher()

        dispatcher.startLoadingSince('CL', ult_fecha_chile)
        dispatcher.startLoadingSince('JP', ult_fecha_japon)
        dispatcher.startLoadingSince('US', ult_fecha_usa)

        # La base de datos se actualiza con la última fecha de carga por cada país cuando se almacena la
        # información en la base de datos (Ultima actividad de los InfoFormatters)

        # Se pone el flag de proceso en progreso en False
        set_flag_process_in_progress(False)
