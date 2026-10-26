# Trabajo-Final-Distribuidos

## Decisiones - estado actual

- El client lee .parquet de a 100 rows, decodificando los 7 campos del articulo.
- Ejecucion con 1 client, 1 gateway, 1 filtro Q1 y el load balancer.
- Load balancer asigna gateways a clientes por Round-Robin.
- Cliente envia articulos y recibe resultados concurrentemente por TCP (full duplex).
- El cliente no manda ACKs al recibir resultados para no complicar el protocolo: el propio TCP ya regula la velocidad si el cliente lee lento.
- Gateway publica en las queues: article_dates, article_sections y abstracts.
- Gateway soporta multicliente.
- Filtro Q1 consume article_dates y confirma los mensajes.
- Filtro publica coincidencias y EOF en results_0.
- Cliente preparado para recibir y persistir resultados de Q1 a Q5.
- Lista vacia indica EOF de entrada y de resultados.
- Stopwords: Go no trae listas en la libreria estandar, asi que se copiaron las de NLTK (las mismas que usa el notebook) a `tokenizer/stopwords.go`.

## Pendientes

- Probar levantar multicliente.
- Validar multiples gateways con el load balancer.
- Implementar nodos de Q2, Q3, Q4 y Q5.
- Gateway: adaptar ResultCollector para recibir resultados de Q2 a Q5.

## Consultas

- Se puede utilizar la libreria externa parquet-go?
- Se puede utilizar la libreria externa `github.com/bbalet/stopwords` para las stopwords?
- Que proyecciones enviará Gateway a los filtros y al Tokenizer?
  Por ahora Gateway proyecta en las queues: article_dates (language, url, date_modified) hacia q1, article_sections (language, name, url, date_modified, image, sections) hacia q2 y abstracts (language, url, date_modified, abstract) hacia el tokenizer.
- Como avisa cada etapa al Collector que termino? Y si no hay coincidencias?
  Por ahora cada etapa envia un mensaje con lista vacia indicando EOF, incluso si no hubo coincidencias.

  Problemas de usar lista vacia como EOF:
  - Si un filtro recibe un lote y ningun articulo cumple la condicion, la lista resultante queda vacia. Si publica esa lista vacia, la etapa siguiente cree que termino todo el archivo por error.
  - El Collector recibira 5 mensajes de eof por cliente (uno por cada consulta: Q1 a Q5) y no debe cerrar la sesion hasta que terminen todas (como hace ahora).

  Solucion: Tipos de mensaje (enums) -> mas escalable y legible

- Como retornan los resultados de Q1 a Q5 hacia el Collector?
  Por ahora solo Q1 publica en `results_<gatewayID>`.

  Solucion por ahora: Una sola queue compartida: requiere agregar un `queryID` en el mensaje (`[queryID, clientID, records]`) para saber que deserializar (el gateway recibe ese parametro en results)

- Es necesario mandar los acks por aplicación en la ida?
