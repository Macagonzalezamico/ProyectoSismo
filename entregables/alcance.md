# Alcance

## Dentro del alcance

### Datos de origen

El servicio que se entregará va a funcionar sobre la base del tratamiento y análisis de información sísmica detectada en los siguientes territorios: Chile, Estados Unidos y Japón.

Los servicios de datos a utilizar son:

- Chile: https://www.sismologia.cl/sismicidad/catalogo

- Estados Unidos: https://earthquake.usgs.gov/fdsnws/event/1/query

- Japón: https://www.data.jma.go.jp/multi/quake/index.html?lang=es

La información a ser analizada se recopilará desde el 1 de Enero del año NNNN. `TODO: Definir el año`.

Nuestro servicio buscará novedades cada NN minutos (`TODO: Definir la periodicidad`) y las agregará a la base de datos, quedando la misma disponible para realizar análisis.

### Base de datos

El servicio contará con una base de datos donde la información se almacenará según un criterio de estandarización en común para los tres paises.

### Web de consultas

El servicio contará con una web que permitirá realizar consultas según los objetivos definidos (Ver documento objetivos.md).

## Fuera del alcance

Queda fuera del alcance de este proyecto la generación de un feed, como así también la generación de alertas a teléfonos celulares.
