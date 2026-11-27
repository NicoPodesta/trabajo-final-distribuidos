#!/usr/bin/env python3
import argparse
import os
import random
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE_DEFAULT = os.path.join(RAIZ, "golang", "docker-compose.yaml")
CLIENT_RESTART_DELAY = 3.0


def ahora():
    return datetime.now().strftime("%H:%M:%S")


def ejecutar(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def contenedores_activos(compose_file):
    resultado = ejecutar([
        "docker", "compose", "-f", compose_file,
        "ps", "--status", "running", "--format", "{{.Name}}",
    ])
    if resultado.returncode != 0:
        raise RuntimeError(resultado.stderr.strip() or "No se pudo consultar docker compose ps")
    return sorted(linea.strip() for linea in resultado.stdout.splitlines() if linea.strip())


def coincide_prefijo(nombre, prefijos):
    return any(nombre == p or nombre.startswith(p) for p in prefijos)


def filtrar_objetivos(contenedores, excluir):
    monitores_vivos = [c for c in contenedores if c.startswith("monitor_")]
    objetivos = []

    for nombre in contenedores:
        if nombre == "rabbitmq" or nombre.startswith("client_") or nombre.startswith("gateway_"):
            continue
        if excluir and coincide_prefijo(nombre, excluir):
            continue
        objetivos.append(nombre)

    return objetivos, monitores_vivos


def elegir_victimas(objetivos, monitores_vivos, cantidad):
    monitores_objetivo = [c for c in objetivos if c.startswith("monitor_")]
    otros_objetivos = [c for c in objetivos if not c.startswith("monitor_")]

    max_monitores_a_matar = max(0, len(monitores_vivos) - 1) if monitores_vivos else 0
    candidatos = list(otros_objetivos)
    if max_monitores_a_matar > 0:
        candidatos.extend(monitores_objetivo)

    random.shuffle(candidatos)
    victimas = []
    monitores_elegidos = 0

    for nombre in candidatos:
        if len(victimas) >= cantidad:
            break
        if nombre.startswith("monitor_"):
            if monitores_elegidos >= max_monitores_a_matar:
                continue
            monitores_elegidos += 1
        victimas.append(nombre)

    return victimas


def matar_contenedor(nombre):
    resultado = ejecutar(["docker", "kill", "-s", "SIGKILL", nombre])
    if resultado.returncode != 0:
        err = resultado.stderr.strip() or f"exit {resultado.returncode}"
        print(f"[{ahora()}] [ERROR] No se pudo matar {nombre}: {err}", file=sys.stderr, flush=True)
        return False

    print(f"[{ahora()}] SIGKILL -> {nombre}", flush=True)
    return True


def iniciar_contenedor(nombre):
    resultado = ejecutar(["docker", "start", nombre])
    if resultado.returncode != 0:
        err = resultado.stderr.strip() or f"exit {resultado.returncode}"
        print(f"[{ahora()}] [ERROR] No se pudo iniciar {nombre}: {err}", file=sys.stderr, flush=True)
        return False

    print(f"[{ahora()}] START -> {nombre}", flush=True)
    return True


def esperar_interrumpible(segundos, debe_detener):
    fin = time.monotonic() + segundos
    while not debe_detener() and time.monotonic() < fin:
        time.sleep(min(0.2, fin - time.monotonic()))


def instalar_senales():
    estado = {"detener": False}

    def manejar(_signum, _frame):
        estado["detener"] = True

    signal.signal(signal.SIGINT, manejar)
    signal.signal(signal.SIGTERM, manejar)
    return lambda: estado["detener"]


def modo_aleatorio(args):
    detener = instalar_senales()
    conteo = Counter()
    ronda = 0

    print(f"[{ahora()}] Escenario 'random' iniciado (intervalo={args.interval}s)", flush=True)

    while not detener():
        try:
            contenedores = contenedores_activos(args.compose)
        except RuntimeError as err:
            print(f"[{ahora()}] [WARN] {err}", file=sys.stderr, flush=True)
            esperar_interrumpible(args.interval, detener)
            continue

        objetivos, monitores_vivos = filtrar_objetivos(contenedores, args.exclude)
        victimas = elegir_victimas(objetivos, monitores_vivos, cantidad=1)

        ronda += 1
        if not victimas:
            print(f"[{ahora()}] Ronda #{ronda}: sin objetivos elegibles activos.", flush=True)
        elif matar_contenedor(victimas[0]):
            conteo[victimas[0]] += 1

        esperar_interrumpible(args.interval, detener)

    print(f"\n[{ahora()}] Simulador detenido. Total SIGKILL enviados: {sum(conteo.values())}", flush=True)
    for nombre, veces in conteo.most_common():
        print(f"  - {nombre}: {veces}", flush=True)
    return 0


def modo_catastrofe(args):
    contenedores = contenedores_activos(args.compose)
    objetivos, monitores_vivos = filtrar_objetivos(contenedores, args.exclude)
    victimas = elegir_victimas(objetivos, monitores_vivos, cantidad=len(objetivos))

    if not victimas:
        print(f"[{ahora()}] No hay contenedores elegibles activos para catastrofe.", flush=True)
        return 0

    sobrevivientes = [m for m in monitores_vivos if m not in victimas]
    print(f"[{ahora()}] Escenario 'catastrophe': matando {len(victimas)} contenedor(es) (monitores preservados: {sobrevivientes})", flush=True)

    muertos = sum(1 for nombre in victimas if matar_contenedor(nombre))
    print(f"[{ahora()}] Catastrofe finalizada: {muertos}/{len(victimas)} contenedores terminados.", flush=True)
    return 0


def modo_cliente_gateway(args):
    print(f"[{ahora()}] Escenario 'client-gateway' iniciado", flush=True)
    contenedores = contenedores_activos(args.compose)
    clientes = [c for c in contenedores if c.startswith("client_")]
    gateways = [c for c in contenedores if c.startswith("gateway_")]

    if clientes:
        cliente = random.choice(clientes)
        if matar_contenedor(cliente):
            try:
                time.sleep(CLIENT_RESTART_DELAY)
            finally:
                iniciar_contenedor(cliente)
    else:
        print(f"[{ahora()}] Sin client_* activos.", flush=True)

    if gateways:
        matar_contenedor(random.choice(gateways))
    else:
        print(f"[{ahora()}] Sin gateway_* activos.", flush=True)

    print(f"[{ahora()}] Escenario 'client-gateway' finalizado.", flush=True)
    return 0


def monitor_lider(monitores_vivos):
    if len(monitores_vivos) <= 1:
        return None
    ordenados = sorted(monitores_vivos, key=lambda n: int(n.rsplit("_", 1)[1]))
    return ordenados[-1]


def modo_eleccion_lider(args):
    print(f"[{ahora()}] Escenario 'leader' iniciado", flush=True)
    contenedores = contenedores_activos(args.compose)
    monitores = [c for c in contenedores if c.startswith("monitor_")]
    lider = monitor_lider(monitores)

    if lider is None:
        print(f"[{ahora()}] Se necesitan al menos 2 monitores activos (activos: {monitores}).", flush=True)
    else:
        matar_contenedor(lider)

    print(f"[{ahora()}] Escenario 'leader' finalizado.", flush=True)
    return 0


def parse_lista(valor):
    return [item.strip() for item in valor.split(",") if item.strip()] if valor else []


def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulador de fallas mediante SIGKILL sobre contenedores del compose.",
    )
    parser.add_argument(
        "--scenario",
        choices=["random", "client-gateway", "catastrophe", "leader"],
        default="random",
        help="Escenario a simular: random (default), client-gateway, catastrophe o leader",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Segundos entre bajas en 'random' (default: 5)",
    )
    parser.add_argument(
        "--exclude",
        type=parse_lista,
        default=[],
        help="Prefijos extra a excluir en 'random'/'catastrophe' (ej: q1_,q2_,q3_)",
    )
    parser.add_argument(
        "--compose",
        default=COMPOSE_DEFAULT,
        help=f"Ruta al docker-compose.yaml (default: {COMPOSE_DEFAULT})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.interval <= 0:
        print("Error: --interval debe ser positivo", file=sys.stderr)
        return 1

    try:
        if args.scenario == "catastrophe":
            return modo_catastrofe(args)
        if args.scenario == "client-gateway":
            return modo_cliente_gateway(args)
        if args.scenario == "leader":
            return modo_eleccion_lider(args)
        return modo_aleatorio(args)
    except RuntimeError as err:
        print(f"[{ahora()}] [ERROR] {err}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
