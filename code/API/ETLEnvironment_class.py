# Este archivo contiene la clase ETLEnvironment.
# ATENCION! Este archivo debe agregarse al .gitignore y adaptarse según el ambiente donde se haya instalado el software.

class ETLEnvironment():
    '''
    Esta clase se ha definido para proveer información del ambiente en donde se encuentra instalado el software.
    '''

    def __init__(self) -> None:

        # La siguiente propiedad debe identificar el path previo al del directorio del proyecto
        # Ejemplo:
        #  Si el proyecto está instalado en /repo/ProyectoSismo, entonces esta propiedad contendrá /repo/
        #self.root_project_path = '/DefinaAquiElPathDondeElProyectoEstaInstalado/'
        self.root_project_path = 'code/API/'
