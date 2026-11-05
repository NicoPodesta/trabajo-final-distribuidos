# Informe de Arquitectura y Escalabilidad

Sistemas Distribuidos (75.74) — TP Escalabilidad

**Integrantes:** Arian Jarmolinski, Nicolas Podesta y Gabriel Re.

En este informe explicamos cómo repartimos el trabajo entre los componentes, cómo se comunican y cómo coordinamos los EOFs.

Los diagramas de la carpeta `Diagramas/` acompañan estas explicaciones.

---

## 1. Propuesta general

El sistema procesa artículos de Wikipedia en inglés y francés y ejecuta cinco consultas en paralelo. Los datos y resultados de cada cliente se mantienen separados.

Procesamos la entrada en batches y comunicamos las etapas mediante un middleware propio sobre **RabbitMQ**. Enviamos los resultados a medida que están listos; las consultas que necesitan reunir todos los datos esperan a completar su cálculo.

### Consultas del sistema

| Consulta | Resultado y Criterio |
| :--- | :--- |
| **Q1** | URL y fecha de modificación de artículos en inglés hasta 2020 inclusive. Evaluación independiente por artículo (*map/filter* desacoplado). |
| **Q2** | Nombre, URL e imagen en bytes de artículos hasta 2020 con sección *biography* o *biographie*. Descarga asíncrona vía API externa de Wikimedia y entrega fragmentada en *chunks*. |
| **Q3** | Palabras distintas compartidas entre abstracts en inglés y francés. Requiere sharding consistente por palabra para resolver la intersección de vocabularios. |
| **Q4** | Top 20 de palabras más frecuentes en abstracts, excluyendo stopwords y contando cada término una sola vez por abstract. Conteo particionado por réplica y consolidación global mediante un merger. |
| **Q5** | URL y cantidad de palabras del abstract de artículos desde 2021 que superan el promedio histórico hasta 2020 (con el criterio de conteo de Q4). Requiere cálculo previo del promedio global y retención temporal de artículos recientes. |

Podemos atender varios clientes al mismo tiempo y agregar réplicas en las etapas con más trabajo.

---

## 2. Componentes y decisiones principales

| Componente | Responsabilidad | Decisión de Escalabilidad |
| :--- | :--- | :--- |
| **Client** | Lee archivos Parquet por batches y los envía por TCP. Al mismo tiempo recibe resultados, guarda los CSV y reconstruye las imágenes en disco. | Varios clientes pueden trabajar al mismo tiempo, cada uno con sus archivos de entrada y salida. |
| **Load Balancer** | Es el primer contacto del cliente. Le devuelve la dirección de un Gateway para que se conecte directamente a él. | Reparte los clientes por turnos (*round-robin*) entre los Gateways configurados. No interviene en el envío posterior de artículos y resultados. |
| **TCP Handler / Gateway / Collector** | Mantiene la conexión con el cliente, envía a cada consulta los campos que necesita y devuelve los resultados. Maneja las sesiones, confirmaciones y cierres. | Podemos tener varios Gateways, cada uno atendiendo clientes concurrentes. Comparten una cola de resultados por Gateway y distinguen a los clientes por su sesión. |
| **Q1 Filter** | Filtra artículos históricos en inglés y publica las coincidencias hacia el Gateway del cliente, sin esperar a terminar toda la entrada. | Los filtros comparten una cola y se reparten los batches. Coordinan el EOF por conteo para no cerrar mientras otra réplica sigue procesando. |
| **Q2 Filter & Image Fetcher** | Los filtros seleccionan biografías históricas y generan pedidos de descarga. Los fetchers descargan las imágenes a temporales y las publican por chunks. | Los filtros reparten pedidos entre fetchers con colas propias. Cada fetcher tiene varios descargadores y un publicador, permitiendo descargar imágenes mientras envía otra. |
| **Tokenizer** | Pasa el texto a minúsculas, quita stopwords y tokens solo numéricos. Prepara el vocabulario para Q3, los conteos para Q4 y las cantidades de palabras para Q5. | Las réplicas comparten la cola de abstracts y coordinan el EOF por conteo de batches. Reparten las salidas según lo que necesita cada consulta. |
| **Q3 Vocabulary Join** | Guarda por cliente las palabras vistas en cada idioma y publica las que aparecen en inglés y francés, sin repetir resultados. | Repartimos por hash de palabra para que ambos idiomas lleguen al mismo join. Cada join tiene su cola y mantiene su parte del vocabulario en memoria. |
| **Q4 Word Counter & Top Merger** | Los contadores acumulan en cuántos abstracts aparece cada palabra y arman sus tops parciales. El merger los reúne para obtener el top 20 global. | Cada palabra va a un único contador. Así repartimos los conteos y el merger recibe solo los tops parciales, en lugar de todas las palabras. |
| **Q5 Avg Partial, Aggregator & Filter** | Los parciales suman palabras y artículos históricos; el aggregator calcula el promedio global y lo envía a los filtros, que seleccionan los recientes que lo superan. | Los parciales y los filtros se reparten sus respectivas entradas. Los filtros guardan candidatos en disco hasta recibir el promedio, evitando acumularlos todos en memoria. |
| **Middleware** | Reúne el manejo de colas, exchanges, conexiones y confirmaciones de RabbitMQ. También permite limitar los mensajes entregados sin confirmar. | Compartimos este código para no repetirlo en cada componente. Usamos colas para repartir trabajo y exchanges para enviar avisos a varias réplicas. |

---

## 3. Diseño general: de C4 al flujo y al despliegue

Los diagramas van desde una vista general del sistema hasta el detalle de sus componentes y mensajes:

Contexto C4 → DAG → Despliegue → Robustez → Paquetes → Actividades → Secuencia

Cada vista muestra una parte del diseño: quién usa el sistema, cómo pasan los datos, dónde corre cada componente y cómo se coordinan.

### 3.1. Contexto C4: el sistema y su entorno

Complementa al diagrama en [01-contexto.puml](Diagramas/C4Model/01-contexto.puml).

El usuario aporta archivos locales de Wikipedia en inglés y francés y recibe los resultados de las cinco consultas. Solo necesita conectarse al Load Balancer, que le indica qué Gateway usar; no necesita conocer los componentes internos.

Q2 también depende de servidores externos: descarga cada imagen desde la dirección incluida en el artículo. Una descarga puede fallar o tardar, por lo que tiene tiempos y reintentos limitados.

Varios clientes pueden procesar sus archivos al mismo tiempo. Sus datos y resultados se mantienen separados, aunque compartan componentes del sistema.

### 3.2. DAG: transformación y dependencias de los datos

Complementa al diagrama en [tp-disenio-dag.mmd](Diagramas/DAG/tp-disenio-dag.mmd).

El grafo sigue los datos desde su ingreso por el Gateway hasta la devolución de resultados por el Collector:

* **Separación de la entrada**: El Gateway envía solo los campos que necesita cada rama, evitando repetir el artículo completo:
  * Idioma, URL y fecha para **Q1**.
  * Datos del artículo, secciones y referencia de imagen para **Q2**.
  * Abstracts y sus datos asociados para el **Tokenizer**.

* **Procesamiento de las consultas**:
  * **Q1**: Filtra cada artículo de forma independiente y publica las coincidencias. La coordinación entre filtros se necesita para saber cuándo terminaron todos.
  * **Q2**: Primero selecciona las biografías y después descarga sus imágenes. Así no hacemos pedidos a Internet para artículos que no cumplen el filtro.
  * **Q3**: Reparte por palabra para reunir sus apariciones en inglés y francés en el mismo join. Cuando aparece en ambos idiomas, la publica sin esperar al EOF.
  * **Q4**: Cada contador espera a completar su entrada antes de calcular el top parcial. El Merger reúne esos tops y obtiene el top 20 global.
  * **Q5**: Los históricos hasta 2020 aportan al promedio. Los recientes, desde 2021, quedan guardados hasta recibir ese valor y poder compararlos.

* **Retorno de resultados**:
  Todos los mensajes conservan la identidad del cliente. Las cinco consultas publican hacia la cola de su Gateway y el Collector encuentra la sesión correspondiente. Los resultados de distintas consultas pueden llegar intercalados por el mismo socket TCP.

### 3.3. Despliegue: procesos, réplicas y recursos

Complementa al diagrama en [Despliegue.png](Diagramas/Despliegue/Despliegue.png).

Los componentes corren en contenedores Docker:

* Cada cliente tiene sus carpetas de entrada y salida montadas como volúmenes.
* El Load Balancer recibe las conexiones iniciales y asigna un Gateway.
* Varios Gateways atienden a los clientes y conectan la entrada y los resultados con RabbitMQ.
* Un broker **RabbitMQ** compartido comunica las etapas dentro de la red de Docker.
* Las cantidades de filtros, tokenizers, fetchers, joins y contadores son configurables. Se mantienen fijas durante una ejecución para que coincidan el reparto de datos y los cierres esperados.
* Dentro de cada Gateway se atienden conexiones mientras el Collector recibe resultados. Los filtros y tokenizers consumen datos y avisos de coordinación. Cada fetcher tiene un receptor, varios descargadores concurrentes y un publicador de chunks.
* Q2 usa disco para las imágenes temporales y Q5 para los candidatos que esperan el promedio. Q3 conserva el vocabulario y Q4 los conteos en memoria hasta terminar el trabajo de cada cliente.

---

## 4. Diagramas de diseño interno

### 4.1. Diagrama de Robustez del ingreso (conexión y sesión)

Complementa al diagrama en [Diagrama-robustez-ingreso.png](Diagramas/Robustez/Diagrama-robustez-ingreso.png).

* El **Load Balancer** recibe la conexión inicial del cliente y le retorna el host y puerto del Gateway que atenderá su ciclo de vida.
* El **TCP Handler** del Gateway asignado preserva el socket TCP, genera un identificador único de sesión (`clientID`), desmultiplexa los batches recibidos y devuelve confirmaciones (*ACK*).
* El Gateway actúa simultáneamente como **Collector**, abriendo colas de retorno vinculadas a ese cliente y enviando los resultados al cliente a través del socket mientras la ingesta aún puede seguir en curso.

### 4.2. Diagrama de Robustez de las consultas

Complementa a los diagramas en [Diagramas de Robustez Q1 a Q5](Diagramas/Robustez/).

* **Q1 (Filtro histórico)**: Cada worker filtra artículos por idioma y fecha sobre la cola compartida `article_dates`. Coordina el EOF mediante su exchange de control `q1_filter_control` verificando la cantidad total de batches emitidos por el Gateway antes de publicar sus resultados y emitir su respectivo EOF hacia el Collector.
* **Q2 (Filtro y descarga)**: El filtro descarta artículos sin biografía y coordina el EOF mediante `q2_filter_control`. Las descargas se distribuyen entre las réplicas de `Image Fetcher` a través de colas particionadas (`image_downloads_[i]`). Cada Fetcher administra un pool de descargadores concurrentes con mitigación de rate limiting (`HTTP 429`) y almacena temporalmente los archivos en disco mientras emite chunks numerados hacia el Gateway.
* **Q3 (Intersección por palabra)**: Tokenizer emite pares `(word, lang)` particionados mediante hash consistente hacia $K$ colas (`vocabulary_[i]`). Cada réplica mantiene un conjunto en memoria de palabras vistas por idioma y emite la palabra una única vez al detectar la presencia simultánea en inglés y francés, mientras que el Gateway espera el EOF de las $K$ particiones.
* **Q4 (Conteos y top global)**: Tokenizer emite pares `(word, k)` con la cantidad de abstracts del batch que contienen la palabra. Cada `Word Counter` acumula ocurrencias de su shard en memoria RAM. Al completarse la entrada, cada partición envía su top 20 parcial al `Top 20 Merger`, que consolida las listas disjuntas y genera el top 20 global aplicando el mismo criterio de desempate.
* **Q5 (Promedio y candidatos)**: `Avg Partial` acumula suma de palabras y cantidad de abstracts históricos; `Avg Aggregator` consolida los totales parciales y calcula el promedio global. Los artículos recientes se distribuyen entre los `Q5 Filter`, que retienen los candidatos en almacenamiento temporal hasta recibir el promedio definitivo, publicando aquellos que lo superan.

### 4.3. Diagrama de Paquetes

Complementa al [diagrama de paquetes](Diagramas/Paquetes/diagrama-paquetes..png), cuya fuente editable está en [Mermaid](Diagramas/Paquetes/diagrama-paquetes.mmd). Las consultas se presentan como agrupaciones lógicas identificadas por **Q1, Q2, Q3, Q4 y Q5**, con los nombres de sus etapas. Las flechas punteadas indican dependencias de código; la relación entre aplicaciones y paquetes compartidos resume los usos de cada componente.

El código en Go se organiza desacoplando responsabilidades:

* **Cliente** (`client/`): Lectura de archivos Parquet, envío de batches por TCP, recepción de resultados, escritura de CSV y reconstrucción de imágenes.
* `loadbalancer/`: Lógica de asignación y balanceo dinámico de gateways.
* `gateway/`: Manejo de sesiones, TCP handler, collector de resultados y serialización.
* `tokenizer/`: Limpieza de texto, remoción de stopwords, cálculo de métricas y sharding.
* **Q1 - Filter**: Filtro de artículos históricos en inglés.
* **Q2 - Filter - Fetcher**: Filtro de biografías, descarga concurrente de imágenes, almacenamiento temporal y publicación de chunks.
* **Q3 - Vocabulary Join**: Intersección de vocabulario en inglés y francés por partición.
* **Q4 - Counter - Merger**: Conteo de palabras por partición y consolidación del top 20 global.
* **Q5 - Avg - Filter**: Cálculo de promedios parciales, agregación global y filtro de artículos recientes con candidatos pendientes en disco.
* `common/eofcoordinator/`: Protocolo de barrera de sincronización y cierre distribuido con conteo de batches.
* `common/eofcounter/`: Conteo de cierres por cliente en las etapas particionadas que lo utilizan.
* `common/resultpublisher/`: Publicación de resultados hacia la cola del Gateway de cada cliente.
* `common/middleware/`: Abstracción de RabbitMQ (colas con prefetch, exchanges de difusión, direct exchanges con routing keys).
* `common/messageprotocol/`: Protocolos de mensajería (binario para el canal externo TCP, JSON para el canal interno RabbitMQ).
* `common/article/` y `common/result/`: Contratos de artículos de entrada, resultados y chunks de imágenes.
* `common/config/` y `common/logger/`: Configuración de procesos y logs.
* `common/shutdown/`: Manejo unificado de señales del sistema operativo (`SIGTERM`, `SIGINT`).

### 4.4. Diagrama de Actividades

Complementa a los diagramas [actividad q2.png](Diagramas/actividad/actividad%20q2.png) y [actividad q5-filter.png](Diagramas/actividad/actividad%20q5-filter.png) (cuyas fuentes `.puml` se ubican en el mismo directorio).

* **Actividad de Q2 — Descarga y envío de imágenes**:
  Conserva el recorrido por las etapas Q2 Filter e Image Fetcher: selección de artículos, reparto de pedidos entre fetchers, descarga a un temporal, publicación de chunks con el último marcado como final (o nombre y URL sin imagen si falla el origen), eliminación del temporal y confirmación del pedido. Cada fetcher tiene descargadores concurrentes y un único publicador. Las notas resumen el cierre: los filtros coordinan el conteo de batches y cada uno envía su EOF a todos los fetchers; cada fetcher espera esos cierres y resuelve sus pendientes antes de emitir su EOF al Gateway. Los errores internos detienen el fetcher; al apagar se limpian temporales y se cierran conexiones.
  
* **Actividad de Q5 — Filtrado de candidatos**:
  Modela la concurrencia asíncrona sobre tres canales: la llegada de artículos recientes (retenidos en el `pendingStore` en disco hasta contar con el promedio), el arribo del promedio global por el exchange `q5_average` (que dispara la lectura por lotes de 1000 artículos, publicación de coincidencias y eliminación del archivo en disco) y la barrera de control de eof de recientes. Finaliza únicamente al converger el promedio recibido y el EOF de recientes confirmado (`hasAverage && recentsEOF`).

### 4.5. Secuencia: coordinación entre componentes

Complementa a los diagramas [secuencia q2-entrega-imagen-chunks.png](Diagramas/secuencia/secuencia%20q2-entrega-imagen-chunks.png) y [secuencia q5-promedio.png](Diagramas/secuencia/secuencia%20q5-promedio.png).

* **Secuencia de Q2**: Muestra la delegación desde el filtro al Fetcher, la descarga externa vía HTTPS a un archivo local temporal, la entrega fragmentada en chunks hacia el Gateway con confirmación por ACK a nivel aplicación TCP desde el cliente por cada trozo, y la liberación final del archivo en disco y cupo en el Fetcher.
* **Secuencia de Q5**: Ilustra la ingesta paralela de históricos (hacia los partials) y recientes (hacia los filters con persistencia temporal en disco), el cálculo y difusión del promedio global por parte del Aggregator, el posterior drenado y evaluación de candidatos, y el cumplimiento de la condición dual (promedio recibido y fin de recientes) antes de emitir el EOF final hacia el Gateway.

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
* Los mensajes se dirigen directamente a la cola de la partición correspondiente: `vocabulary_[i]` para Q3 (asegurando que las palabras en inglés y francés de un mismo término colisionen en la misma réplica de Join) y `word_counts_[i]` para Q4.
* Cada réplica procesa exclusivamente su conjunto disjunto de términos, logrando escalabilidad lineal con respecto a la cantidad de particiones tanto en la intersección de vocabularios (Q3) como en el conteo de frecuencias (Q4).

### 5.3. Escalabilidad y tolerancia a latencia externa (Image Fetcher)

La consulta Q2 involucra una operación fuertemente ligada a I/O sobre una API externa pública (Wikimedia vía HTTPS) sujeta a latencias de red y cuotas de uso:

* **Escalamiento horizontal**: El clúster distribuye la carga entre $N$ réplicas de `Image Fetcher` mediante colas particionadas (`image_downloads_[i]`), repartiendo el consumo de ancho de banda y el uso de almacenamiento temporal.
* **Concurrencia vertical**: Cada contenedor de Fetcher ejecuta internamente un pool de goroutines de descarga concurrentes (`Q2_DOWNLOAD_WORKERS`). Mientras un worker espera el RTT de red de Wikimedia, otros descargan en paralelo o transfieren chunks a RabbitMQ.

### 5.4. Escenarios de escalabilidad y validación de resultados

El comportamiento del clúster fue evaluado a través de cinco escenarios representativos de carga y topología:

* **Escalamiento horizontal en cómputo**: Tokenizers, filtros de Q1 y contadores de Q4 distribuyen el trabajo en réplicas concurrentes sobre colas compartidas o particionadas, eliminando cuellos de botella en la ingesta masiva de abstracts y artículos.
* **Concurrencia multicliente y aislamiento**: Los **Escenarios 3 y 4** ejecutan múltiples clientes independientes en paralelo (2 y 3 clientes), verificando la asignación balanceada por el Load Balancer, el aislamiento estricto de sesiones (`clientID`) y la convergencia de resultados en sus respectivos Gateways.
* **Validación determinística**: La exactitud funcional de todas las consultas se verifica de forma automatizada mediante un script de comparación (`scripts/comparar.py`) que contrasta los outputs generados por el sistema distribuido contra el Jupyter Notebook de referencia provisto por la cátedra (`scripts/analisis_cliente.ipynb`), asegurando coincidencia estricta en el filtrado, los conteos, el criterio de desempate del Top 20 y el cálculo del promedio global.

---

## 6. Coordinación distribuida de EOF

Complementa a los diagramas [secuencia eof-difusion.png](Diagramas/secuencia/secuencia%20eof-difusion.png) y [secuencia eof-cierre-tokenizers.png](Diagramas/secuencia/secuencia%20eof-cierre-tokenizers.png) (cuyas fuentes `.puml` se ubican en el mismo directorio).

### 6.1. Protocolo de Barrera Distribuida (EOFCoordinator)

Para evitar condiciones de carrera sin importar el orden de llegada de los mensajes ni las diferencias de velocidad entre réplicas, se diseñó un protocolo de **barrera de sincronización distribuida**. Este mecanismo opera a través de exchanges dedicados de control (`tokenizer_control`, `q1_filter_control`, `q2_filter_control`, `q5_avg_partial_control` y `q5_filter_control`):

0. **Handshake inicial (`WaitReady`)**:
   Antes de habilitar el consumo de batches desde la cola de trabajo compartida, cada réplica sondea a sus pares (`msgProbe`) por el exchange de control y aguarda sus respuestas (`msgReady`). Esto garantiza que todas las réplicas del grupo estén activas y suscriptas a sus colas de control antes de que comience el flujo de datos o arribe cualquier EOF.

1. **Conteo en el productor (Gateway)**:
   El productor contabiliza la cantidad exacta de batches de datos emitidos por consulta (`abstractBatches`, `datesBatches`). Al recibir `EndOfRecords` del cliente, incluye en el EOF el total acumulado (`totalBatches`).

2. **Elección dinámica de la réplica coordinadora**:
   La primera réplica que extrae el EOF de la cola compartida asume dinámicamente ese rol para ese cliente, registrando el total esperado.

3. **Fase de sondeo (`msgAskCounts`)**:
   El coordinador difunde por el exchange de control interno una solicitud de conteo a todas las réplicas del grupo.

4. **Reporte local y transición a estado de cierre (`closing`)**:
   Cada réplica recibe `msgAskCounts`, marca su estado local en fase de cierre (`closing = true`) y responde con un mensaje `msgReportCount` indicando cuántos batches procesó y confirmó hasta ese instante.

5. **Resolución de batches residuales en vuelo**:
   Si una réplica termina de procesar un batch de datos que ya tenía asignado con anterioridad, su callback `OnBatchProcessed` detecta que el cliente está en fase de cierre, emite los resultados correspondientes hacia downstream y envía de inmediato un reporte actualizado: `msgReportCount`.

6. **Cumplimiento estricto de la barrera de commit (`msgCountedEOF`)**:
   El coordinador registra los reportes individuales (`reports[replica] = count`). La barrera no se libera hasta que se cumplen simultáneamente dos condiciones:
   * **Cobertura total**: Se recibió el reporte de todas las réplicas del grupo.
   * **Exactitud de la suma con los batches totales**: La suma de los batches procesados por todas las réplicas coincide exactamente con el total emitido por el productor.
   Al cumplirse ambas, se tiene la certeza de que **no queda ningún batch pendiente en ninguna réplica**, y el coordinador difunde el commit definitivo: `msgCountedEOF`.

7. **Propagación segura (`eofcounter`)**:
   Al recibir el commit `msgCountedEOF`, todas las réplicas ejecutan su callback de cierre, liberan su memoria local y emiten su respectivo EOF a las siguientes etapas. Aquellas etapas consumidoras (como `Q3 Vocabulary Join`, `Q4 Word Counter` o el propio Gateway) utilizan contadores (`eofcounter`) para aguardar los EOFs de todas las réplicas antes de considerar concluida la entrada y consolidar sus resultados.

---

## 7. Graceful Shutdown

El sistema asegura un apagado ordenado frente a señales `SIGTERM` o `SIGINT`:

* Cada componente registra manejadores a través del módulo unificado [`common/shutdown`](file:///home/nicop/uba/distribuidos/trabajo-final-distribuidos/golang/src/common/shutdown).
* Al recibir la señal, los nodos detienen inmediatamente el consumo de nuevos mensajes (`StopConsuming()`).
* Se permite que los mensajes actualmente en procesamiento dentro de los workers terminen de ejecutarse y hagan su respectivo ACK/NACK.
* Se cierran ordenadamente los canales y conexiones de RabbitMQ y sockets TCP con los clientes.
* Se eliminan los archivos temporales remanentes en disco (imágenes en tránsito y candidatos de Q5), asegurando que el host quede en un estado limpio.

## 8. Aclaraciones sobre las decisiones

### Coordinación por consulta

* **Q1:** Contamos cada batch después de publicar sus resultados, incluso si no hubo coincidencias. Los reportes son acumulados por réplica: recibir uno repetido no aumenta el total.
* **Q2:** Cada filtro manda su EOF a todos los fetchers. Cada fetcher espera esos cierres y termina sus imágenes pendientes antes de cerrar. Confirma los EOFs parciales para que no ocupen el cupo e impidan recibir los restantes.
* **Q3:** Cada tokenizer manda su EOF a todos los joins, aunque no les haya enviado palabras. Los joins cuentan cierres por cantidad; a diferencia de Q1 y Q2, no identifican cierres duplicados.
* **Q4:** Alcanza con reunir los tops locales: una palabra fuera del top 20 de su contador ya tiene veinte palabras por delante y no puede entrar al global.
* **Q5:** Sumamos palabras y cantidad de artículos, no promediamos los promedios parciales. Cada filtro espera tanto el promedio como el cierre de recientes y la evaluación de sus pendientes.

### Protocolos y sesiones

* TCP lleva mensajes binarios con tipo y longitudes; completamos las lecturas porque un mensaje puede llegar en partes. RabbitMQ lleva JSON para poder inspeccionar los mensajes con facilidad.
* El ACK del Gateway confirma la publicación del batch; el del fetcher, la publicación del resultado; el del cliente, su escritura. Son pasos distintos.
* Hay una cola de resultados por Gateway y sesiones en memoria para encontrar el socket de cada cliente. El EOF de entrada no cierra la conexión: esperamos a que terminen todas las consultas. Si el cliente se desconecta, los resultados posteriores se descartan.

### Límites

* RabbitMQ, el Collector y los servidores de imágenes pueden limitar el rendimiento. Más réplicas no garantizan una mejora proporcional. Las cantidades de réplicas son fijas en el docker compose.
