## Desiciones - estado actual

- El client lee .parquet de a 100 rows,
- Ejecucion con 1 client, 1 gateway y 1 filtro
- Client envia secuancial, sin hilos
- Gateway publica JSON en article_dates
- Filtro Q1 consume article_dates y confirma los mensajes
- Filtro publica coincidencias y fin en results_0
- Client guarda los resultados y termina
- Lista vacia indica fin de entrada

## Pendientes
- Agregar multiclient en client
- Validar multiclient del Gateway
- Coordinar envio y recepcion TCP
- Publicar resultados y fin en results_0

## Consultas
- Se utiliza la libreria parquet-go
- Que proyecciones enviara Parser a los filtros y al Tokenizer?
- Como avisa cada etapa al Collector que termino? Y si no hay coincidencias?
