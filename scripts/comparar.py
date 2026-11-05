#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from collections import Counter

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def leer_lineas(ruta):
    with open(ruta, encoding="utf-8") as archivo:
        return [linea.rstrip("\n") for linea in archivo if linea.strip()]


def pares_sistema(ruta):
    pares = []
    for linea in leer_lineas(ruta):
        clave, valor = linea.rsplit(",", 1)
        pares.append((clave, int(valor)))
    return pares


def pares_notebook(ruta):
    with open(ruta, newline="", encoding="utf-8") as archivo:
        filas = list(csv.reader(archivo))
    return [(clave, int(valor)) for clave, valor in filas[1:]]


def filas_q2(ruta):
    with open(ruta, newline="", encoding="utf-8") as archivo:
        return [(fila["url"], fila["name"]) for fila in csv.DictReader(archivo)]


def comparar_filas(esperado, obtenido):
    esperadas, obtenidas = Counter(esperado), Counter(obtenido)
    if esperadas == obtenidas:
        return "OK", f"{len(obtenido)} filas"
    solo_notebook = sum((esperadas - obtenidas).values())
    solo_sistema = sum((obtenidas - esperadas).values())
    return "DIFF", f"{solo_notebook} filas solo en el notebook, {solo_sistema} solo en el sistema"


def comparar_palabras(esperado, obtenido):
    esperadas, obtenidas = set(esperado), set(obtenido)
    repetidas = len(obtenido) - len(obtenidas)
    if esperadas == obtenidas and not repetidas:
        return "OK", f"{len(obtenido)} palabras"
    return "DIFF", (
        f"{len(esperadas - obtenidas)} palabras solo en el notebook, "
        f"{len(obtenidas - esperadas)} solo en el sistema, {repetidas} repetidas en el sistema"
    )


def solo_empates(esperado, obtenido):
    if not esperado or len(esperado) != len(obtenido):
        return False
    corte = min(valor for _, valor in esperado)
    if corte != min(valor for _, valor in obtenido):
        return False
    por_encima = lambda pares: {(clave, valor) for clave, valor in pares if valor > corte}
    return por_encima(esperado) == por_encima(obtenido)


def comparar_top(esperado, obtenido):
    if dict(esperado) == dict(obtenido):
        detalle = f"{len(obtenido)} filas"
        if esperado != obtenido:
            detalle += " (el orden difiere solo dentro de empates)"
        return "OK", detalle
    if solo_empates(esperado, obtenido):
        return "EMPATE", "mismas palabras por encima del corte; cambia cuales de las empatadas entran al final"
    return "DIFF", "el top 20 no coincide"


def comparar_q2(esperado, obtenido):
    urls_esperadas, urls_obtenidas = dict(esperado), [url for url, _ in obtenido]
    de_mas = set(urls_obtenidas) - set(urls_esperadas)
    nombres_distintos = [url for url, nombre in obtenido if url in urls_esperadas and urls_esperadas[url] != nombre]
    repetidas = len(urls_obtenidas) - len(set(urls_obtenidas))
    if de_mas or nombres_distintos or repetidas:
        return "DIFF", f"{len(de_mas)} filas fuera de la seleccion, {len(nombres_distintos)} nombres distintos, {repetidas} repetidas"
    faltan = len(urls_esperadas) - len(urls_obtenidas)
    if faltan and not urls_obtenidas:
        return "PARCIAL", f"0 de {len(urls_esperadas)}: el sistema no produjo filas (Q2 no se ejecuto o no hubo conexion)"
    if faltan:
        return "PARCIAL", f"{len(urls_obtenidas)} de {len(urls_esperadas)}: faltan {faltan} (imagenes que no se pudieron descargar)"
    return "OK", f"{len(urls_obtenidas)} filas"


CONSULTAS = [
    ("Q1", "q1.csv", "q1_results.csv", pares_notebook, pares_sistema, comparar_filas),
    ("Q2", "q2.csv", "q2_results.csv", filas_q2, filas_q2, comparar_q2),
    ("Q3", "q3.csv", "q3_results.csv", leer_lineas, leer_lineas, comparar_palabras),
    ("Q4", "q4.csv", "q4_results.csv", pares_notebook, pares_sistema, comparar_top),
    ("Q5", "q5.csv", "q5_results.csv", pares_notebook, pares_sistema, comparar_filas),
]


def comparar_consulta(consulta, carpeta_notebook, carpeta_sistema):
    nombre, archivo_notebook, archivo_sistema, leer_notebook, leer_sistema, comparar = consulta
    ruta_notebook = os.path.join(carpeta_notebook, archivo_notebook)
    ruta_sistema = os.path.join(carpeta_sistema, archivo_sistema)
    for ruta in (ruta_notebook, ruta_sistema):
        if not os.path.exists(ruta):
            return "FALTA", ruta
    try:
        return comparar(leer_notebook(ruta_notebook), leer_sistema(ruta_sistema))
    except (ValueError, KeyError, csv.Error) as error:
        return "ERROR", f"no se pudo leer un archivo ({error})"


def main():
    parser = argparse.ArgumentParser(description="Compara las salidas del sistema con las del notebook, cliente por cliente.")
    parser.add_argument("--sistema", default=os.path.join(RAIZ, "output"), help="carpeta con client_N/qX_results.csv (por defecto output/)")
    parser.add_argument("--notebook", default=os.path.join(RAIZ, "output_notebook"), help="carpeta con client_N/qX.csv (por defecto output_notebook/)")
    parser.add_argument("--clientes", nargs="+", type=int, default=[0, 1, 2], help="clientes a comparar (por defecto 0 1 2)")
    args = parser.parse_args()

    estados = Counter()
    for cliente in args.clientes:
        for consulta in CONSULTAS:
            estado, detalle = comparar_consulta(
                consulta,
                os.path.join(args.notebook, f"client_{cliente}"),
                os.path.join(args.sistema, f"client_{cliente}"),
            )
            estados[estado] += 1
            print(f"client_{cliente}  {consulta[0]}  {estado:8}  {detalle}")

    print()
    resumen = ", ".join(f"{cantidad} {estado}" for estado, cantidad in sorted(estados.items()))
    hay_error = any(estados[estado] for estado in ("DIFF", "FALTA", "ERROR"))
    print(f"Resumen: {resumen}.", "HAY DIFERENCIAS" if hay_error else "Todo coincide con el notebook.")
    return 1 if hay_error else 0


if __name__ == "__main__":
    sys.exit(main())
