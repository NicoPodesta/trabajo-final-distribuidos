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
| **Q1 Filter** | Filtra artículos históricos en inglés y publica directamente al Gateway del cliente. | Réplicas compitiendo sobre cola compartida. |
| **Q2 Filter & Image Fetcher** | Filtra biografías históricas y gestiona la descarga concurrente de imágenes desde Wikimedia vía HTTPS, fragmentando la entrega en chunks. | Workers compitiendo en paralelo; control de cupos de descarga y archivos temporales acotados. |
| **Tokenizer** | Extrae y normaliza palabras de abstracts sin stopwords. Computa y despacha métricas para Q3, Q4 y Q5. Implementa el coordinador de fin de entrada. | Réplicas compitiendo con coordinación distribuida de EOF. |
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
* Almacenamiento temporal en disco utilizado por Q2 (archivos de imagen mientras se transmiten chunks) y por Q5 (candidatos recientes a la espera del promedio).

---

## 4. Diagramas de diseño interno

### 4.1. Diagrama de Robustez del ingreso (conexión y sesión)

Complementa al diagrama en `Diagramas/Robustez/Diagrama-robustez-ingreso.png`.

* El **Load Balancer** recibe la conexión inicial del cliente y le retorna el host y puerto del Gateway que atenderá su ciclo de vida.
* El **TCP Handler** del Gateway asignado preserva el socket TCP, genera un identificador único de sesión (`clientID`), desmultiplexa los batches recibidos y devuelve confirmaciones (*ACK*).
* El Gateway actúa simultáneamente como **Collector**, abriendo colas de retorno vinculadas a ese cliente y enviando los resultados al cliente a través del socket mientras la ingesta aún puede seguir en curso.

### 4.2. Diagrama de Robustez de las consultas

Complementa a los diagramas en `Diagramas/Robustez/DIagrama-robustez-q1.png` a `q5.png`.

* **Q1 (Filtro independiente)**: Cada worker filtra por idioma y fecha. Mantiene solo el estado en vuelo de los batches en procesamiento y propaga el cierre sin retener estado acumulado.
* **Q2 (Filtro y descarga)**: El filtro descarta artículos sin biografía. `Image Fetcher` resuelve la descarga desde Wikimedia administrando un pool de concurrencia y un archivo temporal por imagen, fragmentando la salida en chunks numerados para que el cliente reconstruya el archivo binario.
* **Q3 (Intersección por palabra)**: Tokenizer emite pares `(word, lang)`. El sharding consistente garantiza que ambos idiomas para una misma palabra lleguen a la misma réplica. Cada réplica mantiene un conjunto en memoria de palabras vistas por idioma y emite la palabra una única vez al detectar la presencia simultánea en inglés y francés.
* **Q4 (Conteos y top global)**: Tokenizer emite pares `(word, k)` con la cantidad de abstracts del batch que contienen la palabra. Cada `Word Counter` acumula ocurrencias de su shard en memoria. Al completarse la entrada, cada partición envía su top 20 parcial al `Top 20 Merger`, que consolida las listas disjuntas y genera el top 20 global aplicando el mismo criterio de desempate.
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
  Combina la llegada asíncrona de candidatos y la del promedio global. Los artículos recientes se almacenan en un `pendingStore`. Al arribar el promedio, se procesa la cola de candidatos pendientes evaluando la condición. El nodo finaliza únicamente cuando se cumplen simultáneamente: fin de entradas, promedio recibido y todos los candidatos evaluados.

### 4.5. Secuencia: coordinación entre componentes

Complementa a los diagramas en `Diagramas/secuencia/secuencia q2-entrega-imagen-chunks.puml` y `secuencia q5-promedio.puml`.

* **Secuencia de Q2**: Detalla la notificación de biografía detectada, solicitud a la API externa, persistencia local, transmisión fragmentada en chunks con metadatos (nombre, URL, correlación) y emisión del mensaje de fin de imagen hacia el Gateway.
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

### 5.3. Saturación de procesamiento mediante Prefetch (QoS)

Para maximizar el throughput de cómputo en workers intensivos (como el Tokenizer y los filtros), las colas de RabbitMQ configuran un `prefetch > 1`. Esto permite que el broker mantenga batches precargados en el búfer del socket local de cada worker, eliminando el tiempo ocioso de espera entre el ACK de un batch y la recepción del siguiente.

---

## 6. Coordinación distribuida de fin de datos (EOF con Prefetch > 1)

Complementa a los diagramas en `Diagramas/secuencia/secuencia eof-difusion.puml` y `secuencia eof-cierre-tokenizers.puml`.

### 6.1. La race condition del EOF

Al utilizar colas de trabajo compartidas entre múltiples réplicas con `prefetch > 1`:

1. El Gateway finaliza el envío de batches y emite el mensaje de EOF a la cola compartida.
2. RabbitMQ entrega el EOF a **una sola réplica** (la primera disponible).
3. Las otras réplicas aún conservan batches de datos válidos en su búfer local de prefetch.
4. Si la réplica que recibió el EOF simplemente avisara a las demás para que cierren, las otras réplicas emitirían su propio EOF aguas abajo **antes** de procesar los batches que tenían buffered en memoria local.
5. Esto provocaría pérdida de datos y arribo de registros desordenados luego del EOF en las etapas downstream.

### 6.2. Protocolo de Barrera Distribuida (EOFCoordinator)

Para resolver la race condition preservando el prefetch, se implementó un protocolo de **barrera de sincronización distribuida en dos fases**:

1. **Conteo en el productor (Gateway)**:
   El Gateway contabiliza la cantidad exacta de batches de datos emitidos (`abstractBatches`). Al recibir `EndOfRecords` del cliente, incluye en el mensaje de fin el total acumulado (`totalBatches`).

2. **Elección dinámica de réplica coordinadora**:
   La réplica de Tokenizer que extrae el EOF de la cola compartida asume dinámicamente el rol de **Coordinadora** para ese cliente, registrando `expectedTotal = totalBatches`.

3. **Fase de sondeo (`ask_counts`)**:
   El coordinador difunde por el exchange de control interno (`tokenizer_control`) una solicitud de estado a todas las réplicas del grupo.

4. **Reporte local y transición a estado de cierre (`closing`)**:
   Cada réplica recibe `ask_counts`, marca su estado interno como `closing = true` y responde con un mensaje `report_count` con la cantidad de batches que procesó y confirmó hasta ese instante.

5. **Manejo de prefetch residual (+1)**:
   Si una réplica termina de procesar un batch que ya tenía en su búfer de prefetch luego de haber reportado, su método `OnBatchProcessed` detecta que `closing == true` y emite de inmediato un reporte incremental:
   `ControlMessage(Type: report_count, Count: 1)`.

6. **Barrera de Commit (`commit_eof`)**:
   La réplica coordinadora acumula la suma de reportes (`reportedSum += msg.Count`). La barrera no se libera hasta que se alcanza la igualdad exacta (`reportedSum >= expectedTotal`). En ese momento, se tiene la certeza matemática de que ningún batch quedó pendiente en el prefetch de ninguna réplica, y el coordinador difunde `commit_eof`.

7. **Propagación segura hacia downstream**:
   Al recibir `commit_eof`, todas las réplicas ejecutan su callback de cierre y emiten su respectivo EOF a las colas de las etapas siguientes (`Q3`, `Q4`, `Q5`), garantizando que downstream nunca reciba un EOF antes de finalizar todos los datos.

---

## 7. Graceful Shutdown

El sistema asegura un apagado ordenado frente a señales `SIGTERM` o `SIGINT`:

* Cada componente registra manejadores a través del módulo unificado [`common/shutdown`](file:///home/nicop/uba/distribuidos/trabajo-final-distribuidos/golang/src/common/shutdown).
* Al recibir la señal, los nodos detienen inmediatamente el consumo de nuevos mensajes (`StopConsuming()`).
* Se permite que los mensajes actualmente en procesamiento dentro de los workers terminen de ejecutarse y hagan su respectivo ACK/NACK.
* Se cierran ordenadamente los canales y conexiones de RabbitMQ y sockets TCP con los clientes.
* Se eliminan los archivos temporales remanentes en disco (imágenes en tránsito y candidatos de Q5), asegurando que el host quede en un estado limpio.
