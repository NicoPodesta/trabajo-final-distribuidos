## Desiciones - estado actual

- El client lee .parquet de a 100 rows,
- Implementado 1 client y 1 gateway. Sin multi client
- Client envia secuancial, sin hilos
- Implementacion sin colas
- El gateway solo recibe y loguea

## Pendientes
- Agregar multiclient en client
- Soportar client en Gateway
- 

## Consultas
- Se utiliza la libreria parquet-go
- 