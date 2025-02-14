# Proyecto Final: Diseño y Desarrollo de un Pipeline Completo

1. [Análisis Exploratorio Avanzado (EDA) ](#schema1)
2. [Detección de Outliers en la Interacción de Publicaciones](#schema2)
3. [Análisis Geográfico (Países con Más Sentimientos Negativos o Positivos)](#schema3)
4. [Relación entre Popularidad y Sentimiento](#schema4)
5. [Optimización del Pipeline](#schema5)



<hr>

<a name="schema1"></a>

# 1.Análisis Exploratorio Avanzado (EDA)

En esta sección, aplicarás todos los conocimientos adquiridos para construir un pipeline de datos de principio a fin, optimizando el rendimiento y asegurando buenas prácticas en la manipulación de datos.

- Objetivos del Proyecto Final
    - Diseñar y desarrollar un pipeline de datos que procese información en batch y en tiempo real.
    - Optimizar la ejecución del pipeline usando particionamiento, cacheo y almacenamiento eficiente.
    - Integrar el pipeline con bases de datos relacionales y NoSQL.
    - Documentar y versionar el código en Git.

[Dataset](https://www.kaggle.com/datasets/kashishparmar02/social-media-sentiments-analysis-dataset)

## Datos de entrada:
Se utilizará un dataset de comentarios en redes sociales, con los siguientes campos:
- text: El contenido del comentario.
- sentiment: La clasificación del sentimiento (positivo, neutral, negativo).
- timestamp: Fecha y hora del comentario.
- user_id: Identificador único del usuario.
- platform: Red social de origen (Twitter, Facebook, etc.).

## Pasos seguidos
- Paso 1: Carga y Limpieza de Datos
- Paso 2: Análisis Exploratorio Avanzado (EDA) con PySpark
    - Distribución de sentimientos (positivo, negativo, neutral).
        - Vamos a contar cuántos registros hay en cada categoría de sentimiento (positivo, negativo, neutral).
    - Frecuencia de palabras clave por sentimiento (¿qué palabras aparecen más en cada categoría?).
        - Este paso tokeniza el texto, elimina las stopwords y cuenta las palabras más frecuentes en cada tipo de sentimiento.
    - Tendencias temporales (¿cuándo hay más sentimientos positivos o negativos?).
        - Queremos ver cómo evolucionan los sentimientos a lo largo del tiempo (por año, mes y día).
    - Hashtags más populares por sentimiento.
        - Queremos encontrar los hashtags más populares asociados a cada tipo de sentimiento.

<hr>

<a name="schema2"></a>

# 2. Detección de Outliers en la Interacción de Publicaciones
Podemos identificar tweets o publicaciones con interacciones extremadamente altas en likes y retweets.
Esto ayuda a detectar posibles virales o bots.

- Técnicas:
    - Estadísticas de percentiles (p.ej., valores por encima del percentil 95 son outliers).
    - Desviación estándar (detección de valores atípicos en interacci
- Método de Percentiles (Outliers por Percentil 95)
  - Vamos a calcular el percentil 95 de likes y retweets, y filtraremos publicaciones que superen esos valores.
  - Calcula el percentil 95 de Likes y Retweets usando `percentile_approx().`
  - Filtra publicaciones que superan ese umbral.
  - Ejemplo: Si el percentil 95 de Likes es 5000, cualquier post con más de 5000 likes será un outlier.
- Método de Desviación Estándar (Outliers por Z-Score)
  - Otra opción es detectar publicaciones con interacciones muy por encima de la media (por ejemplo, más de 3 desviaciones estándar).
  - Calcula media y desviación estándar de Likes y Retweets.
  - Filtra posts que superan 3 desviaciones estándar (μ + 3σ).
  - Ejemplo: Si la media de Likes es 500 y la desviación estándar es 1000, un post con más de 3500 Likes será un outlier.




<hr>

<a name="schema3"></a>

# 3. Análisis Geográfico (Países con Más Sentimientos Negativos o Positivos)
Si el dataset incluye el país de origen de cada post, puedes hacer un ranking de países según el sentimiento predominante
- Agrupar los datos por País y Sentimiento: Esto te dará el número de tweets por cada combinación de país y sentimiento.
- Contar la cantidad de tweets por país y sentimiento.
- Ordenar los países por el sentimiento predominante: Aquí podemos ordenar los países para ver cuáles tienen más tweets con un sentimiento positivo o negativo.


<hr>

<a name="schema4"></a>

# 4. Relación entre Popularidad y Sentimiento
Podemos analizar si las publicaciones más populares (con más likes/retweets) tienen una tendencia emocional específica.
- Agrupar los datos por sentimiento: Primero agrupamos por el sentimiento para analizar si existe alguna correlación entre el tipo de sentimiento (positivo, negativo, neutral, etc.) y la popularidad de las publicaciones.
- Calcular el promedio de likes y retweets: Calculamos el promedio de los Likes y Retweets para cada sentimiento. Esto nos ayudará a determinar si existe una tendencia de mayor interacción en ciertos tipos de publicaciones.
- Comparar los resultados: Finalmente, podemos comparar los resultados para responder a las preguntas planteadas.

<hr>

<a name="schema5"></a>


# 5. Optimización del Pipeline

Si el dataset es grande, optimizar el código es clave:
1. Usar cache() para reutilizar DataFrames: Si realizas varias operaciones sobre un DataFrame, es recomendable usar cache() para almacenarlo en memoria y evitar la recomputación.
2. Particionar los datos con repartition() o coalesce(): Repartir los datos en particiones más pequeñas o más grandes puede mejorar el rendimiento, especialmente cuando los datos se procesan en paralelo.
3. Escribir en formato Parquet con compresión Snappy: Parquet es un formato columnar altamente optimizado para almacenar y procesar grandes volúmenes de datos, y Snappy es un algoritmo de compresión rápido que ayuda a reducir el tamaño de almacenamiento sin perder rendimiento en las lecturas/escrituras. en Parquet con compresión Snappy para reducir tamaño de almacenamiento.
