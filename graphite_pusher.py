#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Утилита для заливки данных в Graphite
"""


import argparse
import logging
import socket
import sys
import time

import requests


GRAPH_HOST = "graphite.rn.mf-t"
GRAPH_PORT = 2003
TARGET_HOST = "localhost"
TARGET_PORT = None
TARGET_PATH = "/"
LOG_PATH = "/var/log/graphite_pusher.log"

logging.basicConfig(
    format="%(asctime)s\t%(levelname)s\t%(message)s",
    filename=LOG_PATH,
    level=logging.INFO
)


def get_data(host, port, path, is_ssl=False):
    """Получает данные и генерирует словарь значений, либо возвращает ошибку"""
    ssl = "https" if is_ssl else "http"
    port = ":{}".format(port) if port else ""
    path = "/{}".format(path) if not path.startswith("/") else path

    url = "{}://{}{}{}".format(ssl, host, port, path)
    logging.info("Getting data from {}".format(url))

    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("requests.get {}: {}".format(r.status_code, r.text))

    return r.json()


def parse_data(data, params):
    """Достает из data параметры params и возвращает их значения"""
    response = {}

    for param in params:
        value = data
        param_path = param.split('.')
        param_name = param_path[-1]

        for subparam in param_path:
            if not isinstance(value, dict) or subparam not in value:
                value = None
                break
            value = value.get(subparam)

        if value is not None:
            response[param_name] = value

    return response


def push2graphite(host, port, key, data):
    """Посылает данные в графит"""
    ts = int(time.time())
    logging.info("Sending data to {}".format((host, port)))

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    s.connect((host, port))

    for k, v in data.items():
        if key:
            k = "{}.{}".format(key, k)
        monitoring_parameter = "{} {} {}\n".format(k, v, ts).encode("utf-8")
        try:
            s.send(monitoring_parameter)
        except Exception:
            logging.exception("Graphite error for paramater {}".format(monitoring_parameter))

    s.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Утилита для заливки данных в Graphite.')

    parser.add_argument(
        "-g", "--ghost", action='store', dest='ghost', default=GRAPH_HOST,
    )
    parser.add_argument(
        "-d", "--gport", action='store', dest='gport', default=GRAPH_PORT,
    )
    parser.add_argument(
        "-k", "--key", action='store', dest='gkey', default="",
    )
    parser.add_argument(
        "-s", "--ssl", action='store_true', dest='tssl',
    )
    parser.add_argument(
        "-t", "--host", action='store', dest='thost', default=TARGET_HOST,
    )
    parser.add_argument(
        "-p", "--port", action='store', dest='tport', default=TARGET_PORT,
    )
    parser.add_argument(
        "-u", "--path", action='store', dest='tpath', default=TARGET_PATH,
    )
    parser.add_argument(
        "--param", action='append', dest='params',
    )

    args = parser.parse_args()

    try:
        data = get_data(args.thost, args.tport, args.tpath, is_ssl=args.tssl)
    except Exception as exc:
        logging.exception("get_data failed")
        exit(1)

    try:
        data = parse_data(data, args.params)
    except Exception as exc:
        logging.exception("parse_data failed")
        exit(1)

    try:
        push2graphite(args.ghost, args.gport, args.gkey, data)
    except Exception as exc:
        logging.exception("push2graphite failed")
        exit(1)
