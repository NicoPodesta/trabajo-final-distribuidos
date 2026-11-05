# Verificación contra el notebook

Ejecuta el notebook del enunciado sobre los datos de cada cliente y compara sus resultados con los del sistema distribuido. El notebook corre en un contenedor Docker, así que no hace falta instalar Python ni sus librerías.

Requisitos: Docker, y `python3` (solo para comparar, sin librerías externas).

## Qué hay

| Archivo | Para qué sirve |
|---|---|
| `Dockerfile` | Imagen con Python, pandas, pyarrow, NLTK (con las stopwords incluidas) y Jupyter. Se construye sola la primera vez (`wiki-notebook`) |
| `analisis_cliente.ipynb` | El notebook, con un parámetro `CLIENT` (0, 1 o 2) que elige qué datos lee y dónde exporta |
| `notebook.sh` | Abre Jupyter, o ejecuta el notebook en lote para uno o los tres clientes |
| `comparar.py` | Compara las salidas del sistema con las del notebook, cliente por cliente |

## Paso a paso

**1. Correr el sistema.** Desde `golang/`, `make switch` y `make up` (ver el README de la raíz). Los resultados quedan en `output/client_N/`.

**2. Correr el notebook.** Hay dos maneras:

```bash
scripts/notebook.sh              # abre Jupyter para ejecutar el notebook a mano
scripts/notebook.sh --todos      # ejecuta los tres clientes, sin navegador
scripts/notebook.sh --cliente 1  # ejecuta solo un cliente
```

- *A mano:* el script imprime una URL con token. Abrila en el navegador, abrí `analisis_cliente.ipynb`, poné el número de cliente en `CLIENT` (primera celda) y ejecutá todo con *Kernel → Restart Kernel and Run All Cells*. Se pueden ejecutar las celdas de a una para ver cómo se llega a cada resultado. Para terminar, `Ctrl+C` en la terminal.
- *En lote:* tarda segundos con las muestras de `input/` y muestra solo el progreso.

En los dos casos los resultados quedan en `output_notebook/client_N/` (`q1.csv` a `q5.csv` y `q5_average.txt`; en lote también `executed.ipynb`, con la salida de cada celda). Esa carpeta se genera sola y no se versiona. El notebook que se abre en Jupyter es una copia: lo que ejecutes no modifica el archivo del repositorio.

**3. Comparar.**

```bash
python3 scripts/comparar.py
```

Compara `output/` contra `output_notebook/` para los clientes 0, 1 y 2 y termina con código 1 si hay diferencias. Opciones:

```bash
python3 scripts/comparar.py --clientes 1                           # solo un cliente
python3 scripts/comparar.py --sistema otra/carpeta --notebook otra/carpeta
```

## Cómo leer el resultado

```
client_0  Q1  OK        10114 filas
client_0  Q2  PARCIAL   9 de 10: faltan 1 (imagenes que no se pudieron descargar)
```

| Estado | Significa |
|---|---|
| `OK` | Mismos resultados que el notebook |
| `EMPATE` | Solo Q4: mismas palabras por encima del corte, cambia cuáles de las empatadas en el último puesto entran al top 20. No es un error |
| `PARCIAL` | Solo Q2: todas las filas del sistema están en la selección del notebook, pero faltan algunas. Pasa cuando una imagen no se puede descargar, o si Q2 no se ejecutó. Solo se compara el archivo de salida, no las imágenes |
| `DIFF` | Los resultados no coinciden |
| `FALTA` | No existe uno de los archivos (se muestra cuál) |
| `ERROR` | Un archivo no se pudo leer |

Q1, Q3 y Q5 se comparan como conjuntos de filas, sin importar el orden. Q4 compara el top 20.

## Con otros datos (por ejemplo, el dataset completo)

Por defecto el notebook lee `input/client_N/`. Para leer de otra carpeta:

```bash
TP=/ruta/a/tp_data
INPUT_ROOT=$TP scripts/notebook.sh --cliente 0
```

`INPUT_ROOT` es una carpeta que contiene `client_0`, `client_1` y `client_2`. Si un cliente está en una carpeta con otro nombre, se indica solo ese:

```bash
INPUT_ROOT=$TP CLIENT_2_DIR=$TP/client_3 scripts/notebook.sh --cliente 2
```

El sistema tiene que haber procesado esos mismos datos (se montan en el `docker-compose.yaml` del cliente correspondiente) para que la comparación tenga sentido.

Con datos grandes conviene limitar la memoria del contenedor, para que un notebook descontrolado no afecte al resto de la máquina:

```bash
NOTEBOOK_MEMORY=12g INPUT_ROOT=$TP scripts/notebook.sh --todos
```

Tiempos y memoria medidos con el dataset completo: un cliente de 1,5 millones de artículos tarda unos 4 minutos y usa unos 6 GB; uno de 2,8 millones, unos 7 minutos y unos 9 GB.

## Diferencias con el notebook original

El notebook está adaptado para que sus resultados coincidan con los del sistema (escrito en Go). Lo explica su primera celda:

- Lee los archivos del cliente en vez de bajarlos de Kaggle.
- Reemplaza `İ` por `i` y `Σ` por `σ` antes de pasar a minúsculas. El `lower()` de Python parte la `İ` turca en dos letras y usa `ς` para la sigma al final de palabra; el `ToLower` de Go no.
- El top 20 de Q4 desempata por palabra ascendente (el original, por orden de aparición).
- Carga los archivos de forma más liviana, con los mismos resultados.
- Exporta los resultados a CSV al final.

## Variables

| Variable | Por defecto | Para qué |
|---|---|---|
| `INPUT_ROOT` | `input/` | Carpeta con `client_0`, `client_1` y `client_2` |
| `CLIENT_N_DIR` | `$INPUT_ROOT/client_N` | Carpeta de un cliente en particular |
| `PORT` | `8888` | Puerto de Jupyter en `localhost` |
| `NOTEBOOK_MEMORY` | sin límite | Tope de memoria, por ejemplo `12g` |
| `NOTEBOOK_IMAGE` | `wiki-notebook` | Nombre de la imagen Docker |
