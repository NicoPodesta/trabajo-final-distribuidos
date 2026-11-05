# Informe de Arquitectura y Escalabilidad

Sistemas Distribuidos (75.74) — TP Escalabilidad

**Integrantes:** Arian Jarmolinski, Nicolas Podesta y Gabriel Re.

En este documento describimos el diseño del sistema distribuido, las responsabilidades de sus componentes, los mecanismos de escalabilidad incorporados y las decisiones tomadas para resolver la concurrencia, el particionado y la coordinación de EOFs.

Este informe funciona como documento conceptual y explicativo complementario de los diagramas de diseño ubicados en el directorio `Diagramas/` (C4Model, DAG, Despliegue, Paquetes, Robustez, Actividades y Secuencia).

---

## 1. Propuesta general

El sistema recibe datasets masivos de artículos de Wikipedia en inglés y francés, ejecuta en paralelo cinco consultas y devuelve los resultados a cada cliente de forma aislada y concurrente.

La entrada se procesa en batches acotados y las distintas etapas trabajan en paralelo mediante un middleware propio construido sobre **RabbitMQ**. Los resultados se transmiten en *streaming* hacia el cliente a medida que están disponibles; aquellas consultas que requieren un cómputo o agregación global sincronizan sus barreras de finalización sin bloquear innecesariamente al resto del pipeline.

### Consultas del sistema

| Consulta | Resultado y Criterio |
| :--- | :--- |
| **Q1** | URL y fecha de modificación de artículos en inglés hasta 2020 inclusive. Evaluación independiente por artículo (*map/filter* desacoplado). |
| **Q2** | Nombre, URL e imagen en bytes de artículos hasta 2020 con sección *biography* o *biographie*. Descarga asíncrona vía API externa de Wikimedia y entrega fragmentada en *chunks*. |
| **Q3** | Palabras distintas compartidas entre abstracts en inglés y francés. Requiere sharding consistente por palabra para resolver la intersección de vocabularios. |
| **Q4** | Top 20 de palabras más frecuentes en abstracts, excluyendo stopwords y contando cada término una sola vez por abstract. Conteo particionado por réplica y consolidación global mediante un merger. |
| **Q5** | URL y cantidad de palabras del abstract de artículos desde 2021 que superan el promedio histórico hasta 2020 (con el criterio de conteo de Q4). Requiere cálculo previo del promedio global y retención temporal de artículos recientes. |

El sistema soporta múltiples clientes conectados simultáneamente con sesiones independientes, y permite escalar aumentando las réplicas de cómputo en las etapas que concentran la mayor carga de procesamiento.

---

## 2. Componentes y decisiones principales

| Componente | Responsabilidad | Decisión de Escalabilidad |
| :--- | :--- | :--- |
| **Client** | Lee archivos Parquet por batches, transmite datos por TCP y recibe streams de resultados guardándolos en disco. | Múltiples clientes concurrentes e independientes. |
| **Load Balancer** | Punto de entrada del cluster. Desacopla la red y asigna dinámicamente una réplica de Gateway a cada cliente nuevo. | Distribución balanceada de clientes (*round-robin*). |
| **TCP Handler / Gateway / Collector** | Mantiene la conexión TCP con el cliente, desmultiplexa batches hacia RabbitMQ, reúne resultados de las cinco consultas y maneja confirmaciones (ACK/NACK). | Escalable horizontalmente en $N$ réplicas; sesión aislada por `clientID`. |
| **Q1 Filter** | Filtra artículos históricos en inglés y publica directamente al Gateway del cliente. | Réplicas compitiendo sobre cola compartida con barrera distribuida de control (`q1_filter_control`). |
| **Q2 Filter & Image Fetcher** | Filtra biografías históricas y gestiona la descarga concurrente de imágenes desde Wikimedia vía HTTPS, fragmentando la entrega en chunks. | Filtros coordinados por barrera; Image Fetchers particionados con pool de descargadores concurrentes y mitigación de *rate limit* (`HTTP 429`). |
| **Tokenizer** | Extrae y normaliza palabras de abstracts sin stopwords. Computa y despacha métricas para Q3, Q4 y Q5. Implementa el coordinador de EOF. | Réplicas compitiendo con coordinación distribuida de EOF (`tokenizer_control`). |
| **Q3 Vocabulary Join** | Detecta la presencia de cada palabra en inglés y francés, emitiendo en cuanto se verifica la intersección. | Sharding consistente por hash de palabra ($K$ particiones). |
| **Q4 Word Counter & Top Merger** | Los counters acumulan ocurrencias por palabra en su partición y generan un top 20 parcial. El merger consolida los tops parciales en el top 20 global. | Counters particionados por palabra; Merger como agregador final. |
| **Q5 Avg Partial, Aggregator & Filter** | Los partials suman palabras y cantidad de históricos; el aggregator calcula el promedio global y lo difunde; los filters retienen recientes hasta comparar contra el promedio. | Partials compitiendo; Filters particionados/compitiendo con almacenamiento temporal de candidatos. |
| **Middleware** | Encapsula la comunicación con RabbitMQ (`amqp091-go`): colas de trabajo, direct exchanges, routing keys dinámicas, prefetch y control de conexiones. | Abstracción homogénea e independiente del broker subyacente. |

---

## 3. Diseño general: de C4 al flujo y al despliegue

El diseño se estructura siguiendo el acercamiento progresivo:

Contexto C4 → DAG → Despliegue → Robustez → Paquetes → Actividades → Secuencia

Cada vista responde una pregunta arquitectónica distinta y conserva las responsabilidades de las etapas previas.

### 3.1. Contexto C4: el sistema y su entorno

Complementa al diagrama en `Diagramas/C4Model/01-contexto.puml`.

El usuario interactúa aportando sus archivos locales de Wikipedia en inglés y francés y recibe los resultados de las cinco consultas sin percibir la topología interna.

La API de Wikimedia constituye una dependencia externa consumida vía HTTPS para la descarga de imágenes requeridas por Q2.

El caso de uso modela ejecuciones independientes por cliente, aislamiento estricto de datos entre clientes y procesamiento concurrente y distribuido sobre el cluster.

### 3.2. DAG: transformación y dependencias de los datos

Complementa al diagrama en `Diagramas/DAG/tp-disenio-dag.mmd`.

El grafo sigue los datos desde su ingreso por el Gateway hasta la recolección en el Collector:

* **Bifurcación inicial**: El Gateway proyecta únicamente los campos necesarios para cada rama:
  * Artículos y fechas para **Q1**.
  * Artículos y secciones para **Q2**.
  * Abstracts de texto para el **Tokenizer**.

* **Comportamiento y dependencias de las consultas**:
  * **Q1**: Decide cada artículo de forma local e independiente. No requiere sincronización global.
  * **Q2**: Filtra biografías antes de descargar para reservar ancho de banda de red y disco exclusivamente para imágenes que califiquen.
  * **Q3**: Agrupa palabras por idioma mediante sharding consistente; en cuanto una palabra aparece en ambos idiomas, se emite de inmediato.
  * **Q4**: Requiere que todos los abstracts hayan sido leídos y contados en cada partición antes de que el Merger reciba los tops parciales y arme el top 20 definitivo.
  * **Q5**: Separa artículos históricos (hasta 2020), que aportan al cálculo del promedio global, de los recientes (desde 2021), que son retenidos temporalmente por los filtros hasta recibir dicho valor.

* **Retorno de resultados**:
  Todos los mensajes conservan el identificador del cliente (`clientID`). Las cinco ramas convergen asíncronamente en el Collector asignado a ese cliente, permitiendo entregar resultados intercalados por el socket TCP.

### 3.3. Despliegue: procesos, réplicas y recursos

Complementa al diagrama en `Diagramas/Despliegue/Despliegue.png`.

El despliegue materializa los componentes en unidades de ejecución en contenedores Docker:

* Contenedores cliente independientes montando volúmenes de datasets de entrada y carpetas de salida.
* Contenedor de balanceo de carga (`load_balancer`) como punto de contacto TCP.
* Múltiples instancias de procesos de ingesta (`gateway_0`, `gateway_1`, etc.).
* Broker de mensajería **RabbitMQ** centralizado en la red interna de Docker.
* Conjuntos de réplicas configurables por variable de entorno (`AMOUNT`) para Tokenizers, filtros, descargadores y contadores.
* Cada contenedor explicita sus procesos y goroutines concurrentes (`<<process>>`): hilos de atención TCP y colectores en Gateway; workers de procesamiento y consumidores de control de barrera en Tokenizer y Filtros; y el pipeline interno en Image Fetcher (receptor, pool de descargadores concurrentes y publicador de chunks).
* Almacenamiento temporal en disco utilizado por Q2 (archivos de imagen mientras se transmiten chunks) y por Q5 (candidatos recientes a la espera del promedio), mientras que etapas como Q3 y Q4 procesan exclusivamente en memoria RAM.

---

## 4. Diagramas de diseño interno

### 4.1. Diagrama de Robustez del ingreso (conexión y sesión)

Complementa al diagrama en `Diagramas/Robustez/Diagrama-robustez-ingreso.png`.

* El **Load Balancer** recibe la conexión inicial del cliente y le retorna el host y puerto del Gateway que atenderá su ciclo de vida.
* El **TCP Handler** del Gateway asignado preserva el socket TCP, genera un identificador único de sesión (`clientID`), desmultiplexa los batches recibidos y devuelve confirmaciones (*ACK*).
* El Gateway actúa simultáneamente como **Collector**, abriendo colas de retorno vinculadas a ese cliente y enviando los resultados al cliente a través del socket mientras la ingesta aún puede seguir en curso.

### 4.2. Diagrama de Robustez de las consultas

Complementa a los diagramas en `Diagramas/Robustez/DIagrama-robustez-q1.png` a `q5.png`.

* **Q1 (Filtro histórico)**: Cada worker filtra artículos por idioma y fecha sobre la cola compartida `article_dates`. Coordina el EOF mediante su exchange de control `q1_filter_control` verificando la cantidad total de batches emitidos por el Gateway antes de publicar sus resultados y emitir su respectivo EOF hacia el Collector.
* **Q2 (Filtro y descarga)**: El filtro descarta artículos sin biografía y coordina el EOF mediante `q2_filter_control`. Las descargas se distribuyen entre las réplicas de `Image Fetcher` a través de colas particionadas (`image_downloads_[i]`). Cada Fetcher administra un pool de descargadores concurrentes con mitigación de rate limiting (`HTTP 429`) y almacena temporalmente los archivos en disco mientras emite chunks numerados hacia el Gateway.
* **Q3 (Intersección por palabra)**: Tokenizer emite pares `(word, lang)` particionados mediante hash consistente hacia $K$ colas (`vocabulary_[i]`). Cada réplica mantiene un conjunto en memoria de palabras vistas por idioma y emite la palabra una única vez al detectar la presencia simultánea en inglés y francés, mientras que el Gateway espera el EOF de las $K$ particiones.
* **Q4 (Conteos y top global)**: Tokenizer emite pares `(word, k)` con la cantidad de abstracts del batch que contienen la palabra. Cada `Word Counter` acumula ocurrencias de su shard en memoria RAM. Al completarse la entrada, cada partición envía su top 20 parcial al `Top 20 Merger`, que consolida las listas disjuntas y genera el top 20 global aplicando el mismo criterio de desempate.
* **Q5 (Promedio y candidatos)**: `Avg Partial` acumula suma de palabras y cantidad de abstracts históricos; `Avg Aggregator` consolida los totales parciales y calcula el promedio global. Los artículos recientes se distribuyen entre los `Q5 Filter`, que retienen los candidatos en almacenamiento temporal hasta recibir el promedio definitivo, publicando aquellos que lo superan.

### 4.3. Diagrama de Paquetes

Complementa al diagrama en `Diagramas/Paquetes/diagrama-paquetes..png`.

El código en Go se organiza desacoplando responsabilidades:

* `client/`: Lectura de archivos Parquet, conexión TCP y persistencia de resultados en archivos CSV.
* `loadbalancer/`: Lógica de asignación y balanceo dinámico de gateways.
* `gateway/`: Manejo de sesiones, TCP handler, collector de resultados y serialización.
* `tokenizer/`: Limpieza de texto, remoción de stopwords, cálculo de métricas y sharding.
* `common/eofcoordinator/`: Protocolo de barrera de sincronización y cierre distribuido con conteo de batches.
* `common/middleware/`: Abstracción de RabbitMQ (colas con prefetch, exchanges de difusión, direct exchanges con routing keys).
* `common/messageprotocol/`: Protocolos de mensajería (binario para el canal externo TCP, JSON para el canal interno RabbitMQ).
* `common/shutdown/`: Manejo unificado de señales del sistema operativo (`SIGTERM`, `SIGINT`).

### 4.4. Diagrama de Actividades

Complementa a los diagramas en `Diagramas/actividad/actividad q2.puml` y `actividad q5-filter.puml`.

* **Actividad de Q2 — Descarga y envío de imágenes**:
  El ciclo de vida de una imagen sigue la secuencia: Reservar cupo de descarga → Descargar vía HTTPS → Emitir chunks por RabbitMQ → Eliminar archivo temporal y liberar cupo. El cupo acotado previene que una red externa rápida combinada con un cliente lento sature el disco o la memoria de los workers.

* **Actividad de Q5 — Filtrado de candidatos**:
  Combina la llegada asíncrona de candidatos y la del promedio global. Los artículos recientes se almacenan en un `pendingStore`. Al arribar el promedio, se procesa la cola de candidatos pendientes evaluando la condición. El nodo finaliza únicamente cuando se cumplen simultáneamente: EOF de entradas, promedio recibido y todos los candidatos evaluados.

### 4.5. Secuencia: coordinación entre componentes

Complementa a los diagramas en `Diagramas/secuencia/secuencia q2-entrega-imagen-chunks.puml` y `secuencia q5-promedio.puml`.

* **Secuencia de Q2**: Detalla la notificación de biografía detectada, solicitud a la API externa, persistencia local, transmisión fragmentada en chunks con metadatos (nombre, URL, correlación) y emisión del EOF de imagen hacia el Gateway.
* **Secuencia de Q5**: Muestra el avance paralelo entre la ingesta de históricos (hacia los partials) y recientes (hacia los filters), la consolidación del promedio en el Aggregator y su difusión mediante un exchange hacia todos los Filters.

---

## 5. Estrategia de escalabilidad y concurrencia

### 5.1. Ingreso balanceado con Load Balancer

En lugar de exponer directamente los Gateways a los clientes:

* El Load Balancer actúa como punto único de descubrimiento.
* Monitorea la disponibilidad de los Gateways y asigna conexiones mediante round-robin.
* Permite agregar o remover Gateways según el volumen de clientes simultáneos sin alterar la configuración de los clientes.

### 5.2. Sharding determinístico por hash (FNV-32a)

En Q3 y Q4, las palabras deben agruparse sin que exista un nodo central de coordinación durante la fase de cómputo:

* Tokenizer aplica la función de hash determinística **FNV-32a** sobre cada término: `shard = hash(palabra) % N`.
* El mensaje se dirige directamente a la cola de la partición correspondiente (`word_counts_i`).
* Cada réplica procesa exclusivamente su conjunto disjunto de términos, logrando escalabilidad lineal con respecto a la cantidad de réplicas contadoras.

### 5.3. Escalabilidad híbrida y tolerancia a latencia externa (Image Fetcher)

La consulta Q2 involucra una operación fuertemente ligada a I/O (*I/O-bound*) sobre una API externa pública (Wikimedia vía HTTPS) sujeta a latencias de red y cuotas de uso (*rate limiting*):

* **Escalamiento horizontal**: El clúster distribuye la carga entre $N$ réplicas de `Image Fetcher` mediante colas particionadas (`image_downloads_[i]`), repartiendo el consumo de ancho de banda y el uso de almacenamiento temporal.
* **Concurrencia vertical (*latency hiding*)**: Cada contenedor de Fetcher ejecuta internamente un pool de goroutines de descarga concurrentes (`Q2_DOWNLOAD_WORKERS`). Mientras un worker espera el RTT de red de Wikimedia, otros descargan en paralelo o transfieren chunks a RabbitMQ.
* **Mitigación de *rate limiting* (`HTTP 429`)**: Al detectar saturación de peticiones por parte de la API externa, el descargador aplica pausas y *backoff* para evitar descartes en cascada o reintentos destructivos.

---

## 6. Coordinación distribuida de EOF (Protocolo anti-race condition)

Complementa a los diagramas en `Diagramas/secuencia/secuencia eof-difusion.puml` y `secuencia eof-cierre-tokenizers.puml`.

### 6.1. La condición de carrera del EOF en colas compartidas

En cualquier etapa donde múltiples réplicas consumen concurrentemente de una misma cola de trabajo (como Tokenizer, Q1 o Q5):

1. **Asimetría en la entrega del EOF**: Cuando el productor (por ejemplo, el Gateway) finaliza el envío de datos, publica un único EOF en la cola compartida. RabbitMQ despacha ese mensaje a **una sola réplica** (la primera disponible).
2. **Batches concurrentes en procesamiento**: En ese mismo instante, las restantes réplicas del grupo continúan procesando batches de datos legítimos que fueron extraídos previamente de la cola y que aún se encuentran en vuelo.
3. **Riesgo de terminación prematura**: Si la réplica receptora del EOF simplemente se apagara o difundiera inmediatamente un cierre hacia downstream, las etapas siguientes recibirían el EOF de consulta **antes** de que las demás réplicas terminen de procesar y publicar sus batches pendientes.
4. **Consecuencias**: Esto provocaría pérdida silenciosa de registros, arribo de datos posteriores al cierre y cálculo de resultados globales o agregaciones sobre información incompleta.

### 6.2. Protocolo de Barrera Distribuida (EOFCoordinator)

Para evitar la condición de carrera sin importar el orden de llegada de los mensajes ni las diferencias de velocidad entre réplicas, se diseñó un protocolo de **barrera de sincronización distribuida con conteo de batches**. Este mecanismo es genérico y opera a través de exchanges dedicados de control (`tokenizer_control`, `q1_filter_control`, `q2_filter_control`, `q5_avg_partial_control` y `q5_filter_control`):

0. **Handshake inicial de presencia (`WaitReady`)**:
   Antes de habilitar el consumo de batches desde la cola de trabajo compartida, cada réplica sondea a sus pares (`msgProbe`) por el exchange de control y aguarda sus respuestas (`msgReady`). Esto garantiza que todas las réplicas del grupo estén activas y suscriptas a sus colas de control antes de que comience el flujo de datos o arribe cualquier EOF.

1. **Conteo en el productor (Gateway)**:
   El productor contabiliza la cantidad exacta de batches de datos emitidos por consulta (`abstractBatches`, `datesBatches`). Al recibir `EndOfRecords` del cliente, incluye en el EOF el total acumulado (`totalBatches`).

2. **Elección dinámica de réplica coordinadora**:
   La primera réplica que extrae el EOF de la cola compartida asume dinámicamente el rol de **Coordinadora** para ese cliente, registrando el total esperado (`expectedTotal = totalBatches`).

3. **Fase de sondeo (`msgAskCounts`)**:
   El coordinador difunde por el exchange de control interno una solicitud de conteo a todas las réplicas del grupo.

4. **Reporte local y transición a estado de cierre (`closing`)**:
   Cada réplica recibe `msgAskCounts`, marca su estado local en fase de cierre (`closing = true`) y responde con un mensaje `msgReportCount` indicando cuántos batches procesó y confirmó hasta ese instante.

5. **Resolución de batches residuales en vuelo**:
   Si una réplica termina de procesar un batch de datos que ya tenía asignado con anterioridad, su callback `OnBatchProcessed` detecta que el cliente está en fase de cierre, emite los resultados correspondientes hacia downstream y envía de inmediato un reporte actualizado: `msgReportCount`.

6. **Cumplimiento estricto de la barrera de commit (`msgCountedEOF`)**:
   El coordinador registra los reportes individuales (`reports[replica] = count`). La barrera no se libera hasta que se cumplen simultáneamente dos condiciones:
   * **Cobertura total**: Se recibió el reporte de todas las réplicas del grupo.
   * **Exactitud matemática**: La suma de los batches procesados por todas las réplicas coincide exactamente con el total emitido por el productor ($\sum = \text{expectedTotal}$).
   Al cumplirse ambas, se tiene la certeza matemática de que **no queda ningún batch pendiente en ninguna réplica**, y el coordinador difunde el commit definitivo: `msgCountedEOF`.

7. **Propagación segura y recolección downstream (`eofcounter`)**:
   Al recibir el commit `msgCountedEOF`, todas las réplicas ejecutan su callback de cierre, liberan su memoria local y emiten su respectivo EOF a las etapas downstream. Aquellas etapas consumidoras (como `Q3 Vocabulary Join`, `Q4 Word Counter` o el propio Gateway) utilizan contadores (`eofcounter`) para aguardar el arribo de los EOFs de todas las réplicas upstream antes de considerar concluida la entrada y consolidar sus resultados.

---

## 7. Graceful Shutdown

El sistema asegura un apagado ordenado frente a señales `SIGTERM` o `SIGINT`:

* Cada componente registra manejadores a través del módulo unificado [`common/shutdown`](file:///home/nicop/uba/distribuidos/trabajo-final-distribuidos/golang/src/common/shutdown).
* Al recibir la señal, los nodos detienen inmediatamente el consumo de nuevos mensajes (`StopConsuming()`).
* Se permite que los mensajes actualmente en procesamiento dentro de los workers terminen de ejecutarse y hagan su respectivo ACK/NACK.
* Se cierran ordenadamente los canales y conexiones de RabbitMQ y sockets TCP con los clientes.
* Se eliminan los archivos temporales remanentes en disco (imágenes en tránsito y candidatos de Q5), asegurando que el host quede en un estado limpio.
