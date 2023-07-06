# Este es el inicio del proceso de ETL
# El mismo se comunica con BigQuery para obtener el contexto de ejecución (Flags y fechas)

# Imports
from DataLoadDispatcher_class import DataLoadDispatcher
from google.cloud import bigquery
from google.oauth2 import service_account

def get_google_cloud_client():
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
    
if __name__ == '__main__':

    country_data = [('CL', 'ultima_fecha_chile'), ('JP', 'ultima_fecha_japon'), ('US', 'ultima_fecha_usa')]

    # Obtengo el flag de proceso en curso
    process_in_progress = get_flag_process_in_progress()
    if process_in_progress:
        print('Este proceso no puede ejecutarse porque ya hay un proceso de carga en progreso')
    else:

        # Se pone el flag de proceso en progreso
        set_flag_process_in_progress(True)

        # Se obtienen las ultimas fechas de proceso desde la base de datos

        # Se invoca el dispatcher por cada país y última fecha de carga

        # Se actualiza la base de datos con la última fecha de carga por cada país

        # Se pone el flag de proceso en progreso en False