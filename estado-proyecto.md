## Decisiones - estado actual

- El client lee .parquet de a 100 rows, decodificando los 7 campos del articulo.
- Ejecucion con 1 client, 1 load balancer, 1 gateway y 1 filtro.
- Load balancer asigna gateways a clientes por Round-Robin.
- Client envia articulos y recibe resultados concurrentemente por TCP (full duplex).
- El cliente no manda ACKs al recibir resultados para no complicar el protocolo: el propio TCP ya regula la velocidad si el cliente lee lento.
- Gateway publica en las queues: article_dates, article_sections y abstracts
- Gateway soporta multicliente.
- Filtro Q1 consume article_dates y confirma los mensajes.
- Filtro publica coincidencias y fin en results_0.
- Client guarda los resultados y termina.
- Lista vacia indica fin de entrada y fin de resultados.

## Pendientes
- Probar levantar multicliente.
- Validar multiples gateways con el load balancer.
- Implementar nodos de Q2, Q3, Q4 y Q5.
- Client y gateway: recibir y persistir resultados de Q2, Q3, Q4 y Q5.

## Consultas
- Se puede utiliza la libreria externa parquet-go?
- Que proyecciones enviara Parser a los filtros y al Tokenizer?
Por ahora Gateway proyecta en las queues: article_dates (language, url, date_modified) hacia q1, article_sections (language, name, url, date_modified, image, sections) hacia q2 y abstracts (language, url, date_modified, abstract) hacia el tokenizer.
- Como avisa cada etapa al Collector que termino? Y si no hay coincidencias?
Por ahora cada etapa envia un mensaje con lista vacia indicando fin (EOF), incluso si no hubo coincidencias.
