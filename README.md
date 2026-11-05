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

`cd golang && make switch` permite elegir entre cinco escenarios. Los escenarios 2, 3 y 4 usan tres filtros de Q1, tres filtros de Q2 y tres Fetchers de Q2; el 4 agrega tres Joins de Q3 (los escenarios 1 a 3 tienen uno). El escenario 5 es igual al 4 pero con 25 Fetchers de Q2. El escenario mínimo conserva uno de cada uno. Luego se levanta con `make up`.

## Decisiones - estado actual

### Ejecución

- Ejecucion con cantidades configurables de clientes, gateways, tokenizers, contadores de Q4, Avg Partial y Filter de Q5 filtros de Q1 y Q2, Fetchers de Q2 y Joins de Q3. Un load balancer, el Merger de Q4 y el Aggregator de Q5.

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
- El Gateway espera el cierre de todas las réplicas de la última etapa de cada consulta. En Q1 y Q2 valida quién cierra e ignora cierres repetidos.

### Tokenizer

- Consume batches de `abstracts` desde una cola compartida entre sus réplicas.
- Convierte el texto a minúsculas y elimina stopwords y tokens compuestos solo por números.
- Con `İ` (I turca con punto) `strings.ToLower` de Go da `i`, mientras que `lower()` de Python da `i` más un punto combinante y la palabra se parte en dos (`İdris` pasa a `i` y `dris`). Se conserva el comportamiento de Go. Se modifica el notebook se reemplaza `İ` por `i` antes de pasar a minúsculas.
- Con la `Σ` griega mayúscula pasa algo parecido: `lower()` de Python aplica la regla del sigma final y da `ς` al final de palabra (`ΕΛΛΑΣ` pasa a `ελλας`), mientras que `strings.ToLower` de Go siempre da `σ` (`ελλασ`). Una `ς` que ya viene en minúscula queda igual en los dos. Se conserva el comportamiento de Go y para comparar contra el notebook se reemplaza `Σ` por `σ` antes de pasar a minúsculas. Afecta a unas pocas palabras de Q3 por cada cientos de miles.
- Usa las mismas listas de stopwords de NLTK que el notebook, incluidas en `tokenizer/text/stopwords.go` para mantener el mismo criterio de filtrado.
- Para Q3 publica los pares `(word, language)` sin repetirlos dentro del batch, y los reparte entre las colas `vocabulary_<k>` con `fnv32a(palabra) % Q3_JOIN_AMOUNT`.
- Para Q4 cuenta en cuántos abstracts del batch aparece cada palabra. Cada abstract aporta como máximo 1 por palabra.
- Para Q5 publica la cantidad de palabras de cada artículo, separando históricos hasta 2020 inclusive y posteriores a 2020. Omite los abstracts vacíos. Por cada batch envía **siempre un mensaje** a `historical_counts` y otro a `recent_articles`, vacío si el batch no tiene históricos o recientes, para que el total de mensajes de cada cola sea el de batches.
- El cierre usa el mismo coordinador por conteo que Q1 (ver "Cierre por conteo"). La réplica que recibe el EOF del Gateway, con `total_batches`, coordina; las demás reportan cuántos batches procesaron y publicaron. Cuando la suma coincide con el total, cada réplica publica sus EOF. Antes de consumir datos espera a que todas las réplicas estén suscriptas a `tokenizer_control`.
- Hacia `vocabulary_<k>` y `word_counts_<k>` cada réplica publica su propio EOF (llegan tantos como Tokenizers). Hacia `historical_counts` y `recent_articles` publica un solo EOF, el de la réplica coordinadora, con `total_batches`.

### Q1

- Selecciona artículos en inglés modificados hasta 2020 inclusive, usando la fecha en UTC. Devuelve la URL y la fecha, sin eliminar repetidos.
- Los filtros comparten una cola: cada lote lo procesa una réplica. Los escenarios con réplicas usan tres filtros.
- Antes de consumir, todos esperan que las réplicas estén suscriptas al canal de control para no perder avisos de coordinación.
- Cada filtro publica las coincidencias y recién después cuenta el lote como terminado. Los lotes sin coincidencias también cuentan, pero no generan resultados.
- El Gateway envía un cierre con el total de lotes de ese cliente. El filtro que lo recibe se vuelve coordinador y pide los conteos acumulados de todos.
- Los filtros siguen procesando y actualizando sus conteos durante la coordinación. El coordinador autoriza el cierre cuando todos reportaron y la suma coincide con el total esperado.
- Cada filtro publica su propio cierre después de sus resultados. El Gateway identifica quién cerró, ignora cierres repetidos y termina Q1 cuando cerraron todos.
- Los conteos se separan por cliente y también permiten cerrar entradas vacías. Las réplicas son fijas y el estado queda en memoria; no se recuperan caídas ni se deduplican lotes reentregados.

### Q2

#### Filtros

- Seleccionan artículos modificados hasta 2020 inclusive, con una sección de biografía en inglés o francés. Ignoran mayúsculas y espacios en los extremos de la sección; omiten artículos sin referencia de imagen.
- Comparten la cola de entrada y esperan que todas las réplicas estén listas en el canal de control antes de consumir.
- Por cada coincidencia publican un pedido con nombre, URL del artículo y referencia de imagen. Reparten los pedidos por turnos entre los fetchers, cada uno con su propia cola.
- Cuentan cada lote después de publicar todos sus pedidos, incluso si no produjo ninguno. El filtro que recibe el cierre del Gateway coordina los conteos, como en Q1.
- Cuando todos reportaron y la suma alcanza el total esperado, cada filtro envía su cierre identificado a **todos los fetchers**, después de sus pedidos, aunque no les haya enviado imágenes.

#### Fetchers y coordinación del cierre

- Cada fetcher tiene un receptor de pedidos, varios descargadores y un único publicador trabajando concurrentemente.
- El receptor registra cada imagen pendiente antes de entregarla a los descargadores. El publicador mantiene los pendientes y los cierres recibidos separados por cliente.
- El límite de mensajes recibidos sin confirmar incluye pedidos y cierres; por defecto es cuatro por fetcher. Evita acumular trabajo sin límite, pero no limita los bytes en disco.
- El publicador valida qué filtro envió cada cierre e ignora repetidos. Confirma los cierres parciales para liberar espacio y seguir recibiendo pedidos de los otros filtros.
- Cada fetcher publica su cierre cuando recibió los cierres de todos los filtros y resolvió todas sus imágenes pendientes. También cierra si no recibió imágenes; confirma el último cierre de entrada después de publicar el suyo.
- El Gateway espera un cierre de cada fetcher e ignora duplicados. Así, Q2 no termina mientras otro fetcher todavía descarga o publica imágenes.

#### Descargadores y publicación

- Cada fetcher usa tres descargadores por defecto, configurables. Toman pedidos de un canal compartido: cada pedido lo descarga uno, mientras el publicador puede enviar otra imagen.
- Descargan a archivos temporales y entregan al publicador la ruta y los metadatos. Las imágenes no se pasan completas por memoria.
- El publicador envía fragmentos de hasta 64 KiB, borra el temporal y confirma el pedido. Esa confirmación indica publicación, no que el cliente ya guardó la imagen.
- Cada descarga tiene un presupuesto de cinco segundos y hasta tres reintentos. Los errores 404 y 429 no se reintentan.
- Ante un 429, los descargadores del mismo fetcher comparten una pausa para ese servidor: respetan el tiempo indicado o usan cinco segundos. Durante la pausa, los nuevos pedidos a ese servidor se resuelven sin imagen; la pausa no se comparte entre fetchers.
- Si falla el origen, se publica el nombre y la URL para conservar la fila del CSV sin imagen. Los errores internos detienen el fetcher, limpian temporales y rechazan el trabajo pendiente.
- El cliente guarda los fragmentos en un archivo parcial y lo convierte en imagen completa al recibir el último. Al cerrar elimina las incompletas y avisa si el cierre final llegó con imágenes pendientes.

### Q3

- Recorrido: Tokenizer → `vocabulary_<k>` → Vocabulary Join `k` → `results_<gatewayID>` → Gateway → cliente. Hay `Q3_JOIN_AMOUNT` colas y Joins; cada Join (`Q3_JOIN_ID`) consume solo su cola.
- El Tokenizer envía todos los pares de una palabra al mismo Join (`fnv32a(palabra) % Q3_JOIN_AMOUNT`), así que el inglés y el francés de una palabra siempre se encuentran. Una palabra solo vive en un Join y los resultados de los Joins no se repiten.
- Cada Join envía su propio EOF al terminar de contar `TOKENIZER_AMOUNT` EOF en su cola, y el Gateway espera `Q3_JOIN_AMOUNT` EOF de Q3 (igual que con Q1 y Q5).
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

- Lo usan los filtros de Q1 y Q2, el Tokenizer, el Avg Partial y el Filter de Q5, con `common/eofcoordinator`. Cada réplica cuenta los batches que procesó y publicó. La réplica que recibe el EOF con `total_batches` coordina: pide los conteos, y cuando la suma coincide con el total y reportaron todas, avisa a todas que cierren. El cierre no depende del orden de llegada, así que un batch que una réplica tenía entregado y todavía no procesaba ya no puede quedar después del EOF.
- El coordinador supone un único EOF con total por cliente. Por eso el Tokenizer publica un solo EOF hacia `historical_counts` y `recent_articles`: si cada réplica del Tokenizer publicara el suyo, varias réplicas del Avg Partial se declararían coordinadoras del mismo cliente y el cierre fallaría.
- Cada componente espera a que todas sus réplicas estén suscriptas al canal de control antes de consumir datos. Un mensaje de control que llega a una réplica que todavía no se suscribió se pierde, y el cierre no llegaría nunca. Si una réplica no arranca, las demás no procesan.
- La cantidad de réplicas queda fija durante la ejecución. El estado permanece en memoria. Se cuentan batches procesados, no distintos: una reentrega por una caída no está soportada.
- Q4 y Q3 no lo necesitan: cada contador de Q4 tiene su cola y un solo consumidor, y cada Join de Q3 tiene su cola y un solo consumidor, así que lo que se publica antes del EOF se procesa antes en ambos.

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
