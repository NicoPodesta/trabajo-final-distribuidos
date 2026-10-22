## Desiciones - estado actual

- El client lee .parquet de a 100 rows,
- Implementado 1 client, 1 gateway y 1 filtro. Sin multi client
- Client envia secuancial, sin hilos
- Implementacion sin colas
- El gateway reenvia al filtro por TCP
- El filtro recibe, loguea y confirma cada mensaje

## Pendientes
- Agregar multiclient en client
- Soportar client en Gateway
- Aplicar el filtro de Q1
- Devolver resultados al client
- Pasar Gateway-filtro a RabbitMQ

## Consultas
- Se utiliza la libreria parquet-go
- 