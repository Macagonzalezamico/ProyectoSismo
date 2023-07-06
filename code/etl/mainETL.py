# Este es el inicio del proceso de ETL
# El mismo se comunica con BigQuery para obtener el contexto de ejecución (Flags y fechas)

# Imports
from DataLoadDispatcher_class import DataLoadDispatcher
from google.cloud import bigquery

def get_flag_process_in_progress():
    '''
    Esta función obtiene el flag de proceso en progreso desde la base de datos
    '''

    
if __name__ == '__main__':

    process_in_progress = True
    country_data = [('CL', 'ultima_fecha_chile'), ('JP', 'ultima_fecha_japon'), ('US', 'ultima_fecha_usa')]

    # Obtengo el flag de proceso en curso
    process_in_progress = self.get_flag_process_in_progress()