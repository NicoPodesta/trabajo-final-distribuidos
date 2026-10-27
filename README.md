# Trabajo-Final-Distribuidos

## Decisiones - estado actual

- El client lee .parquet de a 100 rows, decodificando los 7 campos del articulo.
- Ejecucion con 1 client, 1 gateway, 1 filtro Q1 y el load balancer.
- Load balancer asigna gateways a clientes por Round-Robin.
- Cliente envia articulos y recibe resultados concurrentemente por TCP (full duplex).
- El cliente no manda ACKs al recibir resultados para no complicar el protocolo: el propio TCP ya regula la velocidad si el cliente lee lento.
- Gateway proyecta en las queues: `article_dates` (language, url, date_modified) hacia Q1, `article_sections` (language, name, url, date_modified, image, sections) hacia Q2 y `abstracts` (language, url, date_modified, abstract) hacia el Tokenizer.
- Gateway soporta multicliente.
- Filtro Q1 consume `article_dates` y confirma los mensajes.
- Protocolo interno tipado con enums (`MsgData`, `MsgEOF`): se eliminó el uso de lista vacía como EOF para evitar que lotes sin coincidencias se confundan con el fin de transmisión.
- Cola única compartida `results_<gatewayID>`: los mensajes incluyen un header con `type`, `client_id` y `query_id` (Q1 a Q5) para que Gateway sepa qué deserializar.
- Gateway coordina la recepción de resultados de todas las queries: espera el EOF de cada consulta (configurable con `EXPECTED_QUERIES`) antes de enviar el `EndOfRecords` final al cliente y cerrar la sesión.
- Cliente preparado para recibir y persistir resultados de Q1 a Q5.
- Stopwords: Go no trae listas en la libreria estandar, asi que se copiaron las de NLTK (las mismas que usa el notebook) a `tokenizer/stopwords.go`.
- Q4WordCounter - Top parcial: ordena por count descendente y desempata por palabra ascendente.
- Q4WordCounter - mensajes: Se envia a la cola de partial_top20 2 mensajes: top y EOF, asegurando de enviar un solo EOF.

## Pendientes

- Probar levantar multicliente.
- Validar multiples gateways con el load balancer.
- Implementar nodos de Q2, Q3, Q4 y Q5.
- Generalizar y mover `resultPublisher` a `common/` para uso compartido de todos los workers.

## Consultas

- Se puede utilizar la libreria externa parquet-go?
- Se puede utilizar la libreria externa `github.com/bbalet/stopwords` para las stopwords?
- Es necesario mandar los acks por aplicación en la ida?
