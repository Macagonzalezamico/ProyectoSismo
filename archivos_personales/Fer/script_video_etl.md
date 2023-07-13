# Este es el script para el video del ETL

Informar de qué se trata el pipeline del ETL, los módulos que lo conforman y los componentes en juego:

## Funcionalidad

El proceso de ETL permite recuperar información desde diversas fuentes de datos, y a través de flujos de trabajo adaptados a cada caso, procesar y formatear la información, para luego cargarla en la base de datos de Big Query en GCP.

El procesamiento se realiza en lotes con el fin de evitar un exceso en el consumo de recursos. Cada workflow tiene configurado un tamaño de lote acorde a la cantidad de información que el origen de datos ofrece por día.

## Módulos

- Módulos mainETL y DataLoadDispatcher

- Módulos de extracción y formateo por workflow

- Módulo de inserción en Big Query

## Componentes

- Virtual machine Ubuntu en GCP

- Usuario `sismos` para la ejecución del proceso vía automatización por crontab

- Log en /repo/logETL.txt
