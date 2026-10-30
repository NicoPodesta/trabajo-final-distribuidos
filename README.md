# Trabajo-Final-Distribuidos

## Ejecucion local

Los datos de cada cliente van en `input/client_N/` (archivos `.parquet`) y sus resultados quedan en `output/client_N/`.

El `docker-compose.yaml` se genera con `generar-compose.sh`, que recibe el archivo de salida y la cantidad de clientes:

```
./generar-compose.sh golang/docker-compose.yaml 2
cd golang && make up
```

## Decisiones - estado actual

- El client lee .parquet de a 100 rows, decodificando los 7 campos del articulo.
- Ejecucion con 1 client, 1 gateway, el load balancer y una instancia por componente de Q1, Q2, Q4 y Q5.
- Load balancer asigna gateways a clientes por Round-Robin.
- Cliente envia articulos y recibe resultados concurrentemente por TCP (full duplex).
- El cliente no manda ACKs al recibir resultados para no complicar el protocolo: el propio TCP ya regula la velocidad si el cliente lee lento.
- Gateway proyecta en las queues: `article_dates` (language, url, date_modified) hacia Q1, `article_sections` (language, name, url, date_modified, image, sections) hacia Q2 y `abstracts` (language, url, date_modified, abstract) hacia el Tokenizer.
- Gateway soporta multicliente.
- Filtro Q1 consume `article_dates` y confirma los mensajes.
- Filtro Q2 e Image Fetcher funcionando: el filtro consume `article_sections` y publica pedidos individuales en `image_downloads`, el Fetcher descarga y envía una imagen por vez.
- Gateway y cliente soportan `Q2ImageChunk`: se reenvía cada chunk por TCP. Nombre y URL viajan solo en el primer chunk; `last=true` termina una imagen y el EOF Q2 termina la consulta de ese cliente.
- Un `EndOfRecords` con imágenes incompletas es un error. Al salir se cierran los archivos y se eliminan solo los parciales de esta ejecución; las imágenes completas se conservan.
- Protocolo interno tipado con enums (`MsgData`, `MsgEOF`): se eliminó el uso de lista vacía como EOF para evitar que lotes sin coincidencias se confundan con el fin de transmisión.
- Cola única compartida `results_<gatewayID>`: los mensajes incluyen un header con `type`, `client_id` y `query_id` (Q1 a Q5) para que Gateway sepa qué deserializar.
- Q1, Image Fetcher y Q4 TopMerger usan `common/resultpublisher`: cada consulta serializa sus resultados y su EOF; cada worker tiene su publisher, que reutiliza conexiones por gateway y las cierra al salir de `Run`. Si el top está vacío se publica solamente el EOF.
- Gateway coordina la recepción de resultados de todas las queries: espera el EOF de cada consulta (configurable con `EXPECTED_QUERIES`) antes de enviar el `EndOfRecords` final al cliente y cerrar la sesión.
- Cliente preparado para recibir y persistir resultados de Q1 a Q5.
- Stopwords: Go no trae listas en la libreria estandar, asi que se copiaron las de NLTK (las mismas que usa el notebook) a `tokenizer/stopwords.go`.
- Q4WordCounter - Top parcial: ordena por count descendente y desempata por palabra ascendente.
- Q4WordCounter - mensajes: Se envia a la cola de partial_top20 2 mensajes: top y EOF, asegurando de enviar un solo EOF.
- Q5 - promedio entero: el Avg Aggregator envia el promedio como numero entero truncado, no hace falta transmitir decimales.
- Q5 - cliente sin articulos historicos, promedio como 0 que se envia al exchange hacia el q5Filter.
- 1er patron EOF a replicar: una cola por replica con reparto por `hash(clave) % M` y el EOF enviado a todas las colas, para estado por clave. Ej: q4Counter.
- 2er patron EOF a replicar: cola compartida donde la replica que recibe el EOF lo reenvia a todas por un exchange de control, con un mutex para que el EOF salga despues de los datos en curso, replicas sobre una cola. Ej: tokenizer


## Pendientes

- Probar levantar multicliente.
- Validar multiples gateways con el load balancer.
- Implementar Q3.
- Medir descarga y publicación de Q2 antes de agregar concurrencia o réplicas.
- Reutilizar `common/resultpublisher` al implementar las demás querys

## Q2 — funcionamiento actual

1. El filtro selecciona artículos con fecha de modificación hasta 2020 inclusive y una sección llamada `biography` o `biographie`. Ignora mayúsculas y espacios al inicio y al final del nombre de la sección.
2. Cada coincidencia genera un pedido con `client_id`, nombre, URL del artículo y `image_ref`. Esta última es la URL `image.content_url` del Parquet: se descarga directamente, sin consultar otra API.
3. Image Fetcher descarga la imagen completa a un archivo temporal. Luego lo abre y publica chunks de hasta 64 KiB (`CHUNK_SIZE=65536`). No carga la imagen completa en memoria.
4. Cada chunk identifica imagen y secuencia. El primero incluye nombre y URL del artículo; el último lleva `last=true`. Los resultados se publican en `results_<gatewayID>` y Gateway los reenvía por TCP.
5. Tras publicar todos los chunks, el Fetcher cierra y borra el temporal, confirma el pedido con ACK y atiende el siguiente. Este ACK no confirma que el cliente haya guardado la imagen.
6. El EOF de `image_downloads` se publica como EOF de Q2 después de los pedidos anteriores, incluso si no hubo coincidencias. Gateway espera los EOF de Q1, Q2, Q4 y Q5 (`EXPECTED_QUERIES=4`) antes del `EndOfRecords` final.

El cliente guarda las imágenes y un `q2_results.csv` con columnas `image_id,name,url,file` dentro de `output/q2_images/run-*/`. Las imágenes incompletas usan la extensión `.part`; al completarse se renombran según su contenido.

El procesamiento actual es secuencial y usa una instancia del filtro y del Fetcher. Se conservan los chequeos básicos existentes; la concurrencia y el manejo distribuido de errores quedan para otra etapa.

Diagramas del flujo implementado: [actividad](Diagramas/actividad/Actividad%20q2.svg) y [secuencia](Diagramas/secuencia/secuencia%20q2.svg).

### Validación de Q2

Se probó el recorrido completo con Compose, archivos Parquet pequeños y un servidor HTTPS local con imágenes conocidas:

- **Con coincidencias:** de 7 artículos se recibieron las 3 imágenes esperadas. Se comprobó el límite de fecha de 2020, ambas secciones y la normalización de mayúsculas y espacios. Una imagen de 197.069 bytes requirió 4 chunks de 64 KiB como máximo.
- **Sin coincidencias:** 4 artículos generaron un CSV con encabezado y ninguna imagen; el cliente finalizó normalmente.
- Los archivos recibidos coincidieron byte por byte con los originales y el CSV conservó nombres y URLs, incluyendo comas y acentos. Solo se hicieron las 3 descargas esperadas.
- En ambas ejecuciones llegaron los EOF de Q1, Q2, Q4 y Q5. No quedaron archivos `.part`, temporales del Fetcher ni mensajes pendientes en las colas.

Esta prueba verifica el flujo secuencial con un origen HTTPS controlado; no mide rendimiento ni disponibilidad de imágenes en Wikimedia.

## Consultas

- Se puede utilizar la libreria externa parquet-go?
- Se puede utilizar la libreria externa `github.com/bbalet/stopwords` para las stopwords?
- Es necesario mandar los acks por aplicación en la ida?
- Consultar al profe el tamaño de los chunks Q2: por defecto, 64 KiB de contenido (`DefaultQ2ChunkSize`).
- Consultar al profe la salida Q2 prevista: un archivo por imagen y CSV con `image_id,name,url,file`.
