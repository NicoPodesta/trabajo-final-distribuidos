# Informe

**Sistemas Distribuidos TP Diseño**

**Integrantes:** Arian Jarmolinski, Nicolas Podesta y Gabriel Re.

En este documento describimos el diseño propuesto, las responsabilidades de sus componentes y las decisiones tomadas.

## **1. Propuesta general**

Proponemos un sistema que reciba datasets de artículos de Wikipedia en inglés y francés, ejecute las cinco consultas y devuelva los resultados de cada cliente de forma aislada.

La entrada se procesa en lotes acotados y las etapas trabajan en paralelo mediante un middleware propio sobre RabbitMQ. Los resultados se entregan a medida que están disponibles; las consultas que necesitan un cálculo global esperan completar esa dependencia.

**Consultas del sistema**

| Consulta | Resultado |
| ----- | ----- |
| Q1 | URL y fecha de modificación de artículos en inglés hasta 2020 inclusive. |
| Q2 | Nombre, URL e imagen en bytes de artículos hasta 2020 con sección biography o biographie. |
| Q3 | Palabras distintas compartidas entre abstracts en inglés y francés. |
| Q4 | Top 20 de palabras por cantidad de abstracts que las contienen, sin stopwords y contando cada palabra una vez por abstract. |
| Q5 | URL y cantidad de palabras del abstract de artículos desde 2021 que superan el promedio histórico hasta 2020, con el criterio de conteo de Q4. |

Se contempla una ejecución por cliente y múltiples clientes simultáneos. La distribución busca atender tanto más clientes como más datos de un mismo cliente, aumentando las réplicas de las etapas que concentren el trabajo.

## **2. Componentes y decisiones principales**

| Componente | Responsabilidad |
| ----- | ----- |
| Client | Leer la entrada por partes, enviar lotes y guardar los resultados. |
| TCP Handler | Mantener conexiones, recibir mensajes completos y escribir respuestas en el socket correspondiente. |
| Gateway / Collector | Abrir sesiones, publicar lotes y reunir resultados y cierres de cada cliente. |
| Parser / Projector | Normalizar artículos y enviar a cada rama únicamente los campos necesarios. |
| Tokenizer | Preparar el texto una sola vez y agregar los datos utilizados por Q3, Q4 y Q5. |
| Workers de consultas | Filtrar, descargar imágenes, contar palabras y resolver agregaciones. |
| Middleware | Encapsular comunicación, reparto entre réplicas, particionado, difusión y cierre de entradas. |

## **3. Diseño general: de C4 al flujo y al despliegue**

**Contexto C4  → DAG → Despliegue → Robustez → Paquetes → Actividades → Secuencia**

Cada vista responde una pregunta distinta y conserva las responsabilidades de la anterior.

Usamos el enfoque de acercamiento progresivo de C4. Desarrollando el contexto y profundizamos con las vistas complementarias solicitadas.

### **3.1. Contexto C4: el sistema y su entorno**

El usuario aporta su dataset de artículos en inglés y francés y recibe los resultados de las cinco consultas.

La API de Wikimedia es una dependencia externa utilizada para obtener las imágenes seleccionadas por Q2

El caso de uso es una ejecución conjunta de las querys por cliente, consultas aisladas entre clientes y procesamiento distribuido de sus datos. 

### **3.2. DAG: transformación y dependencias de los datos**

El DAG completo sigue los datos desde el ingreso hasta la devolución.

Sus flechas expresan dependencias de procesamiento; una relación lógica puede implementarse mediante colas, exchanges y varias réplicas.

Parser descarta los campos no utilizados, arma el request con los datos necesarios en cada query. Tokenizer filtra las stop words y produce el mensaje con la información tokenizada para el procesamiento de las consultas.

**Comportamiento de las consultas**

**Q1** decide cada artículo de forma independiente.

**Q2** filtra antes de descargar, para reservar red y disco a las imágenes necesarias.

**Q3** reúne la presencia de una palabra en ambos idiomas y puede emitirla apenas se cumple la intersección.

**Q4** necesita completar los conteos por palabra antes de combinar los tops parciales.

**Q5** separa los artículos históricos, que aportan al promedio, de los recientes, que esperan ese valor para poder evaluarse.

El DAG permite reconocer las dependencias entre los flujos de datos. Se observa que hay datos que pueden entregarse a penas se procesan y otros deben esperar como en Q5 Filter

**Retorno de resultados**

Todos los recorridos conservan la identidad del cliente y el Collector de destino.

El retorno converge en Collector, pasa por Handler y llega a Client; los resultados de las distintas consultas pueden llegar intercalados.

### **3.3. Despliegue: procesos, réplicas y recursos**

El diagrama de despliegue lleva los servicios y relaciones anteriores a unidades de ejecución: clientes, procesos de ingreso, broker de mensajes, workers de preparación, filtros, descargadores y agregadores.

Muestra además el acceso HTTPS a Wikimedia y la ubicación de los archivos utilizados por Q2, Q4 y Q5.

El diagrama agrupa varios procesos dentro de algunos contenedores.

Las cantidades de réplicas serán configurables por etapa, y la asignación a máquinas debe permitir aprovechar recursos de distintos equipos.

## **4. Diagramas de diseño interno**

Desarrollamos las responsabilidades, el estado y las condiciones de ejecución que conviene tener presentes al leer cada diagrama.

### **4.1. Diagrama de Robustez del ingreso (conexión y sesión)**

TCP Handler conserva el socket y su conexión con el cliente; Gateway/Collector administra la sesión, los resultados y el cierre de las consultas. 

Client forma batches y comienza a enviarlos después de confirmar la sesión. El TCP handler asigna **client_id** y el gateway asignado queda como Collector.

TCP Handler coordina los socket para no mezclar respuestas. Client puede recibir resultados mientras envía datos.

### **4.2. Diagrama de Robustez de las consultas**

Parser proyecta los campos necesarios y Tokenizer prepara el texto una sola vez.

A partir de las palabras válidas de cada abstract se obtienen counts para Q3, aportes por palabra para Q4 y cantidad de palabras distintas para Q5.

#### **Q1 — Filtro independiente**

Cada worker selecciona artículos en inglés hasta 2020 inclusive y publica URL y fecha.

Mantiene únicamente el trabajo en curso y el control de cierre, incluso si no hubo coincidencias.

#### **Q2 — Filtro y descarga**

El filtro selecciona artículos hasta 2020 con sección biography o biographie, Image Fetcher administra la conexión con la api de wikipedia para las imágenes y maneja un archivo temporal por imagen, los chunks identifican cliente, imagen y secuencia, conservando nombre y URL para reconstruir el resultado.

#### **Q3 — Intersección por palabra**

Tokenizer envía una count de cada par (word,lang) por batch.

El sharding reúne ambos idiomas en el mismo worker, que mantiene por cliente si apareció en inglés, en francés y si ya fue emitida. Al completar ambos idiomas, envía una vez y conserva ese estado hasta el cierre.

#### **Q4 — Conteos y top global**

Tokenizer envía pares (word,k), donde k cuenta los abstracts del lote que contienen esa palabra. Counter los suma por cliente y palabra, al completar los conteos, cada partición envía su top 20 al Merger, que espera todas las listas y obtiene el resultado global.

Esto requiere palabras disjuntas entre réplicas y el mismo método de desempate para confeccionar el top; no se deben tomar tops de lotes antes de reunir sus aportes.

#### **Q5 — Promedio y candidatos**

Los Partial acumulan suma y cantidad de abstracts históricos, aggregator calcula el promedio

Los artículos desde 2021 se reparten entre Filters, que guardan candidatos en archivos por cliente hasta conocer el promedio y publican los que cumplen con el filtro correspondiente

El promedio llega a todos mediante una cola por Filter.

### **4.3. Diagrama de Paquetes**

El diagrama de paquetes agrupa los controles en ingreso, resultados, preparación y consultas; arranque coordina configuración, inicio y apagado.

Los protocolos externo e interno definen los mensajes del TP.

El middleware encapsula reparto, particionado, difusión y cierre de grupos; las reglas de fechas, palabras y promedios permanecen en las consultas.

Serialización, conexiones, contratos y temporales concentran operaciones compartidas.

### **4.4. Diagrama de Actividades**

#### **Actividad de Q2 — Descarga y envío de imágenes**

La actividad de Q2 muestra el ciclo de una imagen:

**Reservar cupo → Descargar → Enviar → Limpiar**

El cupo se conserva hasta terminar las publicaciones y cerrar los archivos; así, una salida lenta no provoca una acumulación ilimitada de temporales.

**Estrategias de descarga y envío**

La descarga se realiza por chunks hasta completarse concurrentemente mientras otra instancia va enviando los datos

#### **Actividad de Q5 — Filtrado de candidatos**

La actividad de Q5 combina llegada de candidatos y promedio.

Al recibir el promedio, Filter debe separar de forma sincronizada lo ya guardado de lo nuevo, evitando omisiones y duplicados.

**Condiciones necesarias para finalizar**

Filter solo puede borrar el archivo y cerrar cuando se cumplen todas las condiciones siguientes:

1. Terminaron las entradas.  
2. Recibió el promedio definitivo.  
3. Recorrió a todos los candidatos.  
4. Publicó los resultados.

### **4.5. Secuencia: coordinación entre componentes**

#### **Secuencia de Q2**

La secuencia de Q2 detalla los mensajes entre filtro, Image Fetcher, cola, Gateway y Client

Actualmente representa las siguientes etapas:

1. Imagen completa.  
2. Aviso de disponibilidad.  
3. Publicación de chunks.  
4. Limpieza.

El aviso de fin debe quedar después de los resultados.

#### **Secuencia de Q5**

La secuencia de Q5 muestra el procesamiento para los candidatos avanzando en paralelo, la reunión de parciales y la difusión del promedio.

Cada Filter evalúa a sus candidatos y avisa al Collector al terminar.

Debe admitir que EOF llegue antes o después del promedio y esperar el procesamiento pendiente, según la condición de actividades.