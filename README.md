# Trabajo-Final-Distribuidos

## Ejecucion local

Los datos de cada cliente van en `input/client_N/` (archivos `.parquet`) y sus resultados quedan en `output/client_N/`.

El `docker-compose.yaml` se genera con `generar-compose.sh`, que recibe el archivo de salida, la cantidad de clientes y, opcionalmente, las cantidades de gateways, tokenizers, contadores de Q4, Avg Partial y Filter de Q5 (por defecto 3 cada una, salvo Avg Partial y Filter de Q5 que son 1):

```bash
./generar-compose.sh <archivo-salida> <clientes> [gateways] [tokenizers] [q4-wordcounters] [q5-avgpartials] [q5-filters]
```

Por ejemplo, con 2 clientes, 2 gateways, 3 tokenizers, 2 contadores de Q4, 2 Avg Partial y 2 Filter de Q5:

```bash
./generar-compose.sh golang/docker-compose.yaml 2 2 3 2 2 2
cd golang && make up
```

`cd golang && make switch` permite generar la configuración eligiendo entre cuatro escenarios fijos. Luego se levanta con `make up`.

## Decisiones - estado actual

### Ejecución

- Ejecucion con cantidades configurables de clientes, gateways, tokenizers, contadores de Q4, Avg Partial y Filter de Q5. Un load balancer y una instancia de los demás componentes de Q1 a Q5.

### Cliente

- El cliente obtiene del Load Balancer la dirección del Gateway y se conecta directamente a él.
- El client lee .parquet de a hasta 100 rows y envía los artículos en batchs (`ArticleBatch`) por TCP al Gateway, esperando un ACK por batch.
- Cliente envia articulos y recibe resultados concurrentemente por TCP (full duplex).
- El cliente envía un ACK por cada resultado recibido y escrito, o por cada chunk de Q2 procesado. Gateway espera ese ACK antes de enviar el siguiente resultado o chunk.
- Al terminar de enviar los archivos, manda `EndOfRecords` y espera su ACK. Sigue recibiendo resultados hasta el `EndOfRecords` del Gateway.

### Load Balancer

- Load balancer asigna gateways a clientes por Round-Robin y devuelve la dirección del Gateway asignado.

### Gateway / Collector / Parser

- Gateway soporta multicliente. Handler, Collector y Parser/Projector
(este último ya no existe como tal) viven en el mismo proceso.
- Cada conexión recibe un `client_id` formado por el ID del Gateway y una secuencia local. Identifica la sesión para devolver los resultados al cliente correspondiente. Las sesiones se mantienen en memoria.
- Gateway proyecta cada batch hacia las colas de RabbitMQ (`article_dates`, `article_sections`, `abstracts`) en un único mensaje por cola conteniendo el batch completo.
- Gateway proyecta en las queues: `article_dates` (language, url, date_modified) hacia Q1, `article_sections` (language, name, url, date_modified, image, sections) hacia Q2 y `abstracts` (language, url, date_modified, abstract) hacia el Tokenizer.
- El ACK de cada batch se envía después de publicar sus proyecciones en las tres colas.
- Protocolo interno tipado con enums (`MsgData`, `MsgEOF`, `MsgChunk`): los batches sin coincidencias no se confunden con el fin de transmisión.
- Cola única por Gateway `results_<gatewayID>`: los mensajes incluyen un header con `type`, `client_id` y `query_id`. Collector los reenvía por el socket de la sesión correspondiente.
- Las cinco consultas usan `common/resultpublisher` para publicar resultados y EOF, reutilizando conexiones por Gateway.
- Gateway espera el EOF de cada consulta (configurable con `EXPECTED_QUERIES`) antes de enviar el `EndOfRecords` final al cliente y cerrar la sesión. Cuenta consultas terminadas, no réplicas.
- Una consulta puede cerrar con más de un EOF: uno por réplica de su última etapa. `Q5_FILTER_AMOUNT` indica cuántos espera de Q5. Como cada réplica publica sus datos y después su EOF por la misma cola, cuando llega el último EOF ya llegaron todos los datos.

### Tokenizer

- Consume batches de `abstracts` desde una cola compartida entre sus réplicas.
- Convierte el texto a minúsculas y elimina stopwords y tokens compuestos solo por números.
- Con `İ` (I turca con punto) `strings.ToLower` de Go da `i`, mientras que `lower()` de Python da `i` más un punto combinante y la palabra se parte en dos (`İdris` pasa a `i` y `dris`). Se conserva el comportamiento de Go. Se modifica el notebook se reemplaza `İ` por `i` antes de pasar a minúsculas.
- Usa las mismas listas de stopwords de NLTK que el notebook, incluidas en `tokenizer/text/stopwords.go` para mantener el mismo criterio de filtrado.
- Para Q3 publica los pares `(word, language)` sin repetirlos dentro del batch, en `vocabulary_0`.
- Para Q4 cuenta en cuántos abstracts del batch aparece cada palabra. Cada abstract aporta como máximo 1 por palabra.
- Para Q5 publica la cantidad de palabras de cada artículo, separando históricos hasta 2020 inclusive y posteriores a 2020. Omite los abstracts vacíos.
- La réplica que recibe el EOF lo distribuye a todas mediante `tokenizer_control`. Cada réplica usa un mutex para esperar a que termine el procesamiento en curso antes de publicar sus EOF hacia las siguientes etapas.

### Q1

- Consume batches de `article_dates` y selecciona artículos con `language = en` y año de modificación hasta 2020 inclusive, interpretando la fecha en UTC.
- Publica `url` y `date_modified` de las coincidencias de cada batch. Un batch sin coincidencias no genera resultados ni termina la consulta.
- No elimina URLs repetidas ni descarta URLs vacías. Un timestamp igual a 0 se interpreta como 1970.
- Si ocurre un error, envía NACK y detiene el consumo.
- Al recibir EOF, publica el EOF de Q1 incluso si no hubo coincidencias y libera las estadísticas del cliente, mantenidas en memoria.
- Actualmente se ejecuta un filtro, todavía falta coordinar el cierre entre réplicas de Q1.

### Q2

- El filtro consume batches de `article_sections` y selecciona artículos con año de modificación hasta 2020 inclusive (UTC) y una sección `biography` o `biographie`, ignorando mayúsculas y espacios en los extremos del nombre de la sección.
- Los artículos sin imagen se omiten y el procesamiento continúa, sin tratarlos como error.
- Cada coincidencia con imagen genera un pedido individual en `image_downloads`, con nombre, URL del artículo e `image_ref`. El Fetcher descarga directamente esa imagen a un archivo temporal y luego publica chunks de hasta 64 KiB con la configuración actual.
- Gateway y cliente soportan `Q2ImageChunk`: se reenvía cada chunk por TCP. Nombre y URL viajan solo en el primer chunk. `last=true` termina una imagen y el EOF Q2 termina la consulta de ese cliente.
- Después de publicar todos los chunks, el Fetcher cierra y borra el temporal y confirma el pedido con ACK. Este ACK no confirma que el cliente haya guardado la imagen.
- El Fetcher usa un receptor, un descargador y un publicador concurrentes dentro de una sola instancia. Puede descargar una imagen mientras publica otra. El canal de pedidos tiene capacidad 1; entre descarga y publicación circulan la ruta y los metadatos del temporal, no sus bytes.
- `Q2_MAX_PENDING_REQUESTS` limita los mensajes de `image_downloads` entregados sin ACK, incluyendo EOF, y fija la capacidad del canal de eventos del publicador. Se configura antes del arranque, vale 4 por defecto y admite valores entre 2 y 65535. Los demás consumidores mantienen su prefetch anterior. Este límite no cambia la cantidad de descargadores ni limita los bytes en disco.
- El publicador registra los pendientes por cliente y es el único que publica chunks y EOF. Recibir EOF marca el fin de entrada; el EOF Q2 se publica cuando no quedan imágenes pendientes de ese cliente, incluso si no hubo imágenes. El ACK del EOF espera esa publicación.
- Los fallos del origen se omiten tras los reintentos (404 no reintenta). Los errores internos cancelan el pipeline: se detiene el consumo, se limpian los temporales pendientes y se hace NACK del trabajo no resuelto. Las imágenes ya publicadas conservan su ACK, aunque falle su limpieza posterior. Las conexiones se cierran después de terminar las etapas.
- Si el cliente recibe el `EndOfRecords` final del Gateway y todavía tiene una imagen sin su último chunk (`last=true`), informa un error. Al cerrar, borra los archivos de las imágenes que quedaron a medio recibir en esa ejecución y conserva las que ya recibió completas.
- El cliente guarda las imágenes en `output/client_N/q2_images/run-*/` y el archivo `q2_results.csv`, con columnas `image_id,name,url,file`, en `output/client_N/`. Mientras recibe los chunks, escribe cada imagen en un archivo `.part`. Al recibir el último chunk, cambia esa extensión por `.jpg`, `.png`, etc., según el formato detectado en los bytes recibidos. Si no reconoce el formato, usa `.bin`.

### Q3

- Recorrido: Tokenizer → `vocabulary_0` → Vocabulary Join → `results_<gatewayID>` → Gateway → cliente. Actualmente hay una cola de vocabulario y un Join.
- Tokenizer elimina pares `(word, language)` repetidos por lote. Q3 conserva `english`, `french` y `sent` por cliente y palabra.
- Una palabra coincide al aparecer en inglés y francés, en cualquier orden, dentro del mismo lote o en lotes distintos.
- `selectedWords` evita duplicados dentro del lote; `sent` evita repetir palabras entre lotes del mismo cliente.
- Publica una lista con las coincidencias nuevas al terminar cada lote, sin esperar EOF. Si no hay coincidencias nuevas, no publica datos.
- Conserva todo el vocabulario en memoria hasta contar `TOKENIZER_AMOUNT` EOF y publicar el EOF de Q3 sin error. También cierra clientes sin coincidencias o sin vocabulario.
- Valida mensajes, cliente, palabras e idiomas. Hace ACK al procesar correctamente; ante errores, NACK con reencolado y detención. Al salir cierra la cola y el publicador.
- Gateway envía cada palabra por TCP y espera su ACK individual. El cliente escribe `q3_results.csv`; recibe el cierre final cuando terminan las consultas esperadas.

### Q4

- Cuenta en cuántos abstracts aparece cada palabra. Cada abstract aporta como máximo 1 por palabra y los contadores suman los valores preagregados de cada batch.
- Tokenizer reparte las palabras con `fnv32a(palabra) % K` (K = `Q4_WORD_COUNTER_AMOUNT`) entre las colas `word_counts_<k>`. Publica datos solo en las colas con palabras y envía EOF a todas.
- Cada WordCounter mantiene sus conteos por cliente en memoria y espera los EOF de los Tokenizers antes de calcular su top 20 parcial.
- El top se ordena por count descendente y desempata por palabra ascendente. Cada contador publica su top en `partial_top20` y luego un EOF. Si no tiene palabras, publica solamente el EOF.
- TopMerger espera los EOF de los K contadores, selecciona el top 20 global con el mismo criterio y publica los resultados y el EOF de Q4. Si el top está vacío, publica solamente el EOF.

### Q5

- Compara la cantidad de palabras de los artículos posteriores a 2020 con el promedio de los artículos hasta 2020 inclusive, usando el año de modificación en UTC y datos separados por cliente.
- Cuenta las palabras después del filtrado del Tokenizer, incluyendo las repetidas. Omite los abstracts vacíos. Un abstract no vacío que queda sin palabras después del filtrado cuenta como 0.
- Avg Partial consume `historical_counts`, acumula suma de palabras y cantidad de artículos, y espera los EOF de los Tokenizers antes de publicar su parcial y EOF en `avg_partials`.
- Avg Aggregator espera los EOF de los parciales y calcula el promedio como entero truncado. Si el cliente no tiene artículos históricos, usa 0. Publica el promedio mediante el exchange `q5_average` hacia los filtros.
- Q5 Filter consume `recent_articles` y guarda los candidatos en memoria hasta recibir el promedio (el objetivo es guardarlos en disco). Selecciona los artículos cuya cantidad de palabras es estrictamente mayor al promedio y publica `url` y `word_count`.
- Las réplicas del Filter compiten por `recent_articles`, así que cada artículo lo recibe una sola réplica, y el promedio llega a todas por una clave de enrutamiento propia (`q5_filter_<id>`). La réplica que recibe un EOF de un Tokenizer lo difunde por `q5_filter_control`, y cada una cuenta un EOF por Tokenizer.
- Cada réplica publica el EOF de Q5 cuando ya recibió el promedio y los EOF de los Tokenizers para los artículos recientes, y libera el estado del cliente. Con varias réplicas el Gateway recibe varios EOF de Q5.

## Pendientes

- Validar la ejecución completa con varios clientes, gateways, tokenizers y comprobando que los resultados y cierres correspondan a cada cliente.
- Carrera en el cierre de los Tokenizers, q5AvgPartial, q5Filter: el EOF de control puede llegar antes de que se procese un lote ya entregado a esa réplica, y ese lote se publica después del EOF.

### Coordinación por secuencias

- Se desarrollo una base común de mensajes (`common/messageprotocol/sequence`) y seguimiento de trabajo terminado (`common/completion`), todavía sin integrar a las consultas.
- Cada mensaje identifica al cliente, al flujo de origen y al lote dentro de ese flujo. Dos filtros que consumen lotes del mismo productor comparten el flujo, aunque procesen lotes distintos.
- Cada productor numera sus lotes desde 1 y anuncia el último número al terminar. Cero indica que no envió lotes; un lote vacío sigue siendo un lote. Los reintentos conservan identidad y contenido.
- El tracker recuerda qué trabajo terminó y qué falta. Recibir el anuncio de fin no alcanza para cerrar: deben completarse todos los números del intervalo, sin contar duplicados ni ignorar huecos.
- Se admite llegada fuera de orden y se rechazan finales contradictorios o números fuera de rango, incluso cuando el anuncio de fin llega después de los datos.

## Consultas

- Se puede utilizar la libreria externa parquet-go? -> solo para la parte de lectura
- json?, busquemos la eficiencia. serializacion de json -> optimizar con respecto a espacios, overhead
- Se puede utilizar la libreria externa `github.com/bbalet/stopwords` para las stopwords? -> Si, adaptando el notebook, adaptar en notebook 'i acento'
- Es necesario mandar los acks por aplicación en la ida? (Se adoptó envío y ack por batches de 100 artículos). -> TCP puede descartar al llenarse los buffers,
recibir no es lo mismo que procesar ACKs distintos capa de transporte(tcp) vs aplicacion.
- Consultar al profe el tamaño de los chunks Q2, tamaño maximo de imagen para descargar, como tratar las imagenes vacias -> No limite de tañamo,
limitar reintentos, si agregamos url e bytes de imagen para poder verificar (no skip de registro en q2_results.csv)
- Consultar al profe la salida Q2 prevista: un archivo por imagen y CSV con `image_id,name,url,file`.
- Para q3 cual es el tamaño minimo de palabras que deberiamos aceptar
- El top se ordena por count descendente y desempata por palabra ascendent
- Graficos
- Prefetch en middleware en 1 para esta entrega OK. Mejora grande, seria intenresante poner mas ej: 20, resolviendo los problemas de coordinacion asociados

## Proximos pasos

- Entrega virtual, 20:30 hs - 45 mins
- Presenten el sistema, presenten el sistema a nivel arquitectura, respuestas del sistema, 3 clientes, 3 replicas en todo minimo, empezar a procesar antes de lanzar
Si quieren meter mas mejor, pero mejor procesar mas DATA, corrida en 20 minutos standarizada. 
- Scripts para comparar los resultados de notebook para realizar la verificacion
- Presentacion no a nivel codigo, sino convenciendo que esto funciona. 
- No es una evalucion oral de la materia, sino del TP
