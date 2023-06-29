# Alcance

## Dentro del alcance

### Datos de origen

El servicio que se entregará va a funcionar sobre la base del tratamiento y análisis de información sísmica detectada en los siguientes territorios: Chile, Estados Unidos y Japón.

Los servicios de datos a utilizar son:

- Chile: https://www.sismologia.cl/sismicidad/catalogo

- Estados Unidos: https://earthquake.usgs.gov/fdsnws/event/1/query

- Japón: https://www.data.jma.go.jp/multi/quake/index.html?lang=es

La información a ser analizada se recopilará para sismos desde el 1 de Enero del año 2000.

La información adicional de impactos económicos y poblacionales se recopilará para sismos ocurridos desde el año 1800.

Nuestro servicio buscará novedades cada hora y las agregará a la base de datos (Carga incremental), quedando la misma disponible para realizar análisis.

### Base de datos

El servicio contará con una base de datos donde la información se almacenará según un criterio de estandarización en común para los tres paises.

### Web de consultas

El servicio contará con una web que permitirá realizar consultas según los objetivos definidos (Ver documento objetivos.md).

Esta web contará con un apartado que permitirá consultar si hay alguna alarma de sismo de gran magnitud.

## Fuera del alcance

Queda fuera del alcance de este proyecto la generación de un feed, como así también la generación de alertas a teléfonos celulares.
