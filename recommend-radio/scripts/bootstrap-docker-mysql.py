"""Provision bounded app and migration accounts on the isolated Docker database."""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pymysql
from sqlalchemy.engine import make_url


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--private-env', type=Path, required=True)
    args = parser.parse_args()
    values = dict(line.split('=', 1) for line in args.private_env.read_text(encoding='utf-8-sig').splitlines() if '=' in line and not line.startswith('#'))
    target = make_url(values['KAFKA_TEST_ROOT_DATABASE_URL'])
    database = values.get('MYSQL_DATABASE', 'radio_acceptance')
    if not re.fullmatch(r'[a-zA-Z0-9_]+', database) or target.host != '127.0.0.1' or target.port != int(values.get('MYSQL_DOCKER_PORT', '13307')):
        raise SystemExit('Refusing a non-isolated database target')
    connection = pymysql.connect(host=target.host, port=target.port, user=target.username,
                                  password=target.password, connect_timeout=5, autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin')
            for prefix, privileges in (('MYSQL_APP', 'SELECT,INSERT,UPDATE,DELETE'), ('MYSQL_ADMIN', 'ALL PRIVILEGES')):
                user, password = values[prefix+'_USER'], values[prefix+'_PASSWORD']
                cursor.execute("CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s", (user, password))
                cursor.execute("ALTER USER %s@'%%' IDENTIFIED BY %s", (user, password))
                cursor.execute("REVOKE ALL PRIVILEGES, GRANT OPTION FROM %s@'%%'", (user,))
                cursor.execute(f"GRANT {privileges} ON `{database}`.* TO %s@'%%'", (user,))
        print('Isolated MySQL accounts provisioned: app=DML, migration=schema owner; no credentials printed.')
    finally:
        connection.close()


if __name__ == '__main__':
    main()
