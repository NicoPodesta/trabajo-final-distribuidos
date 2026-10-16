# Cliente

Como en el TP de coordinación, `main.go` queda como punto de entrada y `client/client.go` la lógica del cliente.

Responsabilidades a implementar:

- Leer el archivo de artículos.
- Conectarse por TCP al componente de ingreso y abrir una sesión.
- Enviar los artículos en batches y luego el EOF.
- Recibir los resultados y la notificación de fin.
