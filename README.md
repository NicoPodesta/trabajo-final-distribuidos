# Trabajo-Final-Distribuidos

## Ejecucion local

Los datos de cada cliente van en `input/client_N/` (archivos `.parquet`) y sus resultados quedan en `output/client_N/`.

El `docker-compose.yaml` se genera con `generar-compose.sh`, que recibe el archivo de salida, la cantidad de clientes y, opcionalmente, las cantidades de gateways, tokenizers, contadores de Q4, Avg Partial y Filter de Q5 y filtros de Q1 y Q2 (por defecto 3 cada una, salvo Avg Partial y Filter de Q5 y filtros de Q1 y Q2, que son 1):

```bash
./generar-compose.sh <archivo-salida> <clientes> [gateways] [tokenizers] [q4-wordcounters] [q5-avgpartials] [q5-filters] [q1-filters] [q2-filters]
```

Por ejemplo, con 2 clientes, 2 gateways, 3 tokenizers, 2 contadores de Q4, 2 Avg Partial y 2 Filter de Q5 y 2 filtros de Q1 y 3 filtros de Q2:

```bash
./generar-compose.sh golang/docker-compose.yaml 2 2 3 2 2 2 2 3
cd golang && make up
```

`cd golang && make switch` permite elegir entre cinco escenarios. Los escenarios 2, 3 y 4 usan tres filtros Q2; el escenario 5 es igual al 4 pero con dos filtros Q2. El escenario mínimo conserva uno. Luego se levanta con `make up`.

## Decisiones - estado actual

### Ejecución

- Ejecucion con cantidades configurables de clientes, gateways, tokenizers, contadores de Q4, Avg Partial y Filter de Q5 y filtros de Q1 y Q2. Un load balancer y una instancia del Fetcher de Q2, Q3, el Merger de Q4 y el Aggregator de Q5.

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
- Una consulta puede cerrar con más de un EOF: uno por réplica de su última etapa. `Q1_FILTER_AMOUNT` y `Q5_FILTER_AMOUNT` indican cuántos espera de cada consulta. En Q1 valida y deduplica por `filter_id`. Cada réplica publica sus datos y después su EOF por el mismo canal hacia la cola de resultados.

### Tokenizer

- Consume batches de `abstracts` desde una cola compartida entre sus réplicas.
- Convierte el texto a minúsculas y elimina stopwords y tokens compuestos solo por números.
- Con `İ` (I turca con punto) `strings.ToLower` de Go da `i`, mientras que `lower()` de Python da `i` más un punto combinante y la palabra se parte en dos (`İdris` pasa a `i` y `dris`). Se conserva el comportamiento de Go. Se modifica el notebook se reemplaza `İ` por `i` antes de pasar a minúsculas.
- Usa las mismas listas de stopwords de NLTK que el notebook, incluidas en `tokenizer/text/stopwords.go` para mantener el mismo criterio de filtrado.
- Para Q3 publica los pares `(word, language)` sin repetirlos dentro del batch, en `vocabulary_0`.
- Para Q4 cuenta en cuántos abstracts del batch aparece cada palabra. Cada abstract aporta como máximo 1 por palabra.
- Para Q5 publica la cantidad de palabras de cada artículo, separando históricos hasta 2020 inclusive y posteriores a 2020. Omite los abstracts vacíos. Por cada batch envía **siempre un mensaje** a `historical_counts` y otro a `recent_articles`, vacío si el batch no tiene históricos o recientes, para que el total de mensajes de cada cola sea el de batches.
- El cierre usa el mismo coordinador por conteo que Q1 (ver "Cierre por conteo"). La réplica que recibe el EOF del Gateway, con `total_batches`, coordina; las demás reportan cuántos batches procesaron y publicaron. Cuando la suma coincide con el total, cada réplica publica sus EOF. Antes de consumir datos espera a que todas las réplicas estén suscriptas a `tokenizer_control`.
- Hacia `vocabulary_0` y `word_counts_<k>` cada réplica publica su propio EOF (llegan tantos como Tokenizers). Hacia `historical_counts` y `recent_articles` publica un solo EOF, el de la réplica coordinadora, con `total_batches`.

### Q1

- Consume batches de `article_dates` y selecciona artículos con `language = en` y año de modificación hasta 2020 inclusive, interpretando la fecha en UTC.
- Publica `url` y `date_modified` de las coincidencias de cada batch. Un batch sin coincidencias no genera resultados ni termina la consulta.
- No elimina URLs repetidas ni descarta URLs vacías. Un timestamp igual a 0 se interpreta como 1970.
- Si ocurre un error, envía NACK y detiene el consumo.
- Al recibir EOF inicia la coordinación y sigue consumiendo. Cuenta cada batch después de publicar sus resultados, incluso si no tuvo coincidencias. Las estadísticas son locales por cliente y réplica, y se liberan después de publicar su EOF exitosamente.
- `Q1_FILTER_ID` identifica la réplica entre 0 y `Q1_FILTER_AMOUNT - 1`. Los valores predeterminados son ID 0, cantidad 1 y `Q1_PREFETCH=1`. Gateway usa la misma cantidad. Compose conserva un filtro.

### Coordinación de Q1

- Gateway cuenta los batches publicados en `article_dates` y envía `total_batches` en su EOF. Cero indica entrada vacía; un total ausente es un error. Gateway y Q1 deben actualizarse juntos.
- Los filtros esperan que todas las réplicas estén suscriptas a `q1_filter_control` antes de consumir datos. El coordinador reúne los conteos acumulados de todas y autoriza el cierre cuando su suma coincide con el total esperado. Cada filtro publica su propio EOF identificado, después de sus resultados.
- La cantidad de réplicas queda fija durante la ejecución. El estado permanece en memoria; se deduplican reportes de control y EOF, no entregas de batches. No se agrega finalizador ni cola de resultados intermedia.

### Q2

- Gateway envía `total_batches` en el EOF de entrada. Los filtros coordinan sus lotes completados y cada uno publica su EOF con `filter_id` después de sus pedidos. Cero se distingue de un campo ausente; Gateway, filtros y Fetcher deben actualizarse juntos.
- `make switch` actualiza la cantidad tanto en los filtros como en el Fetcher.
- El filtro consume batches de `article_sections` y selecciona artículos con año de modificación hasta 2020 inclusive (UTC) y una sección `biography` o `biographie`, ignorando mayúsculas y espacios en los extremos del nombre de la sección.
- Los artículos sin imagen se omiten y el procesamiento continúa, sin tratarlos como error.
- Cada coincidencia con imagen genera un pedido individual en `image_downloads`, con nombre, URL del artículo e `image_ref`. El Fetcher descarga directamente esa imagen a un archivo temporal y luego publica chunks de hasta 64 KiB con la configuración actual.
- Gateway y cliente soportan `Q2ImageChunk`: se reenvía cada chunk por TCP. Nombre y URL viajan solo en el primer chunk. `last=true` termina una imagen y el EOF Q2 termina la consulta de ese cliente.
- Después de publicar todos los chunks, el Fetcher cierra y borra el temporal y confirma el pedido con ACK. Este ACK no confirma que el cliente haya guardado la imagen.
- El Fetcher usa un receptor, un descargador y un publicador concurrentes dentro de una sola instancia. Puede descargar una imagen mientras publica otra. El canal de pedidos tiene capacidad 1; entre descarga y publicación circulan la ruta y los metadatos del temporal, no sus bytes.
- `Q2_MAX_PENDING_REQUESTS` limita los mensajes de `image_downloads` entregados sin ACK, incluyendo EOF, y fija la capacidad del canal de eventos del publicador. Se configura antes del arranque, vale 4 por defecto y admite valores entre 2 y 65535. Los demás consumidores mantienen su prefetch anterior. Este límite no cambia la cantidad de descargadores ni limita los bytes en disco.
- El publicador registra los pendientes por cliente y es el único que publica chunks y EOF. Valida y deduplica los EOF por filtro; confirma los cierres parciales para liberar prefetch y sigue recibiendo pedidos de los otros filtros. Publica un único EOF Q2 cuando cerraron todos y no quedan imágenes pendientes, incluso si no hubo imágenes. El ACK del último EOF espera esa publicación.
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
- Cada WordCounter mantiene sus conteos por cliente en memoria y espera los EOF de los Tokenizers antes de calcular su top 20 parcial. Tiene una cola propia y un solo consumidor, y el EOF viaja por la misma cola que los datos, así que no necesita coordinador: los Tokenizers solo publican sus EOF después de haber publicado todos sus datos.
- El top se ordena por count descendente y desempata por palabra ascendente. Cada contador publica su top en `partial_top20` y luego un EOF. Si no tiene palabras, publica solamente el EOF.
- TopMerger espera los EOF de los K contadores, selecciona el top 20 global con el mismo criterio y publica los resultados y el EOF de Q4. Si el top está vacío, publica solamente el EOF.

### Q5

- Compara la cantidad de palabras de los artículos posteriores a 2020 con el promedio de los artículos hasta 2020 inclusive, usando el año de modificación en UTC y datos separados por cliente.
- Cuenta las palabras después del filtrado del Tokenizer, incluyendo las repetidas. Omite los abstracts vacíos. Un abstract no vacío que queda sin palabras después del filtrado cuenta como 0.
- Avg Partial consume `historical_counts` y acumula suma de palabras y cantidad de artículos. Varias réplicas compiten por la cola. Recibe un único EOF con `total_batches`, cierra por conteo con las demás réplicas (`q5_avg_partial_control`) y recién entonces publica su parcial y su EOF en `avg_partials`.
- Avg Aggregator espera los EOF de los parciales y calcula el promedio como entero truncado. Si el cliente no tiene artículos históricos, usa 0. Publica el promedio mediante el exchange `q5_average` hacia los filtros.
- Q5 Filter consume `recent_articles` y guarda los candidatos hasta recibir el promedio. Los guarda en disco: un archivo por cliente (`<client_id>.jsonl`, una línea por artículo) en `Q5_PENDING_DIR/filter_<id>`, con `Q5_PENDING_DIR` obligatoria. Cuando llega el promedio lee el archivo por tandas de 1000 artículos, publica los que superan el promedio y lo borra. Selecciona los artículos cuya cantidad de palabras es estrictamente mayor al promedio y publica `url` y `word_count`.
- Las réplicas del Filter compiten por `recent_articles`, así que cada artículo lo recibe una sola réplica, y el promedio llega a todas por una clave de enrutamiento propia (`q5_filter_<id>`). Recibe un único EOF con `total_batches` y cierra por conteo con las demás réplicas (`q5_filter_control`).
- Cada réplica publica el EOF de Q5 cuando ya recibió el promedio y el cierre por conteo de los artículos recientes, y libera el estado del cliente. Con varias réplicas el Gateway recibe varios EOF de Q5.

### Cierre por conteo

- Lo usan Q1, el Tokenizer, el Avg Partial y el Filter de Q5, con `common/eofcoordinator`. Cada réplica cuenta los batches que procesó y publicó. La réplica que recibe el EOF con `total_batches` coordina: pide los conteos, y cuando la suma coincide con el total y reportaron todas, avisa a todas que cierren. El cierre no depende del orden de llegada, así que un batch que una réplica tenía entregado y todavía no procesaba ya no puede quedar después del EOF.
- El coordinador supone un único EOF con total por cliente. Por eso el Tokenizer publica un solo EOF hacia `historical_counts` y `recent_articles`: si cada réplica del Tokenizer publicara el suyo, varias réplicas del Avg Partial se declararían coordinadoras del mismo cliente y el cierre fallaría.
- Cada componente espera a que todas sus réplicas estén suscriptas al canal de control antes de consumir datos. Un mensaje de control que llega a una réplica que todavía no se suscribió se pierde, y el cierre no llegaría nunca. Si una réplica no arranca, las demás no procesan.
- La cantidad de réplicas queda fija durante la ejecución. El estado permanece en memoria. Se cuentan batches procesados, no distintos: una reentrega por una caída no está soportada.
- Q4 y Q3 no lo necesitan: cada contador de Q4 tiene su cola y un solo consumidor, y Q3 tiene una sola réplica, así que lo que se publica antes del EOF se procesa antes en ambos.

## Pendientes

- Validar la ejecución completa con varios clientes, gateways, tokenizers y comprobando que los resultados y cierres correspondan a cada cliente.

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
