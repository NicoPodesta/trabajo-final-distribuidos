# Gateway

TCP handler, gateway y parser pertenecen al mismo componente `gateway`.

```text
gateway/
├── main.go                    # Punto de entrada del componente.
├── gateway/
│   └── gateway.go             # Coordinación, sesiones y resultados.
├── tcphandler/
│   └── tcp_handler.go         # Recepción TCP y estado de conexiones.
└── parser/
    └── parser.go              # Lectura y proyección de los artículos.
```