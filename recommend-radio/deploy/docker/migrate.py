"""One-shot account bootstrap and schema migration; never imported by API workers."""
import os
import re

import pymysql
from sqlalchemy.engine import make_url


def main():
    target = make_url(os.environ['DATABASE_URL'])
    database = os.environ.get('MYSQL_DATABASE', 'radio_acceptance')
    if not re.fullmatch(r'[A-Za-z0-9_]+', database) or target.host != 'mysql' or target.database != database:
        raise RuntimeError('Docker migration expects the dedicated mysql service')
    connection = pymysql.connect(host='mysql', port=3306, user='root',
        password=os.environ['MYSQL_ROOT_PASSWORD'], connect_timeout=5, autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin')
            for prefix, privileges in (('MYSQL_APP', 'SELECT,INSERT,UPDATE,DELETE'), ('MYSQL_ADMIN', 'ALL PRIVILEGES')):
                user, password = os.environ[prefix+'_USER'], os.environ[prefix+'_PASSWORD']
                cursor.execute("CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s", (user, password))
                cursor.execute("ALTER USER %s@'%%' IDENTIFIED BY %s", (user, password))
                cursor.execute("REVOKE ALL PRIVILEGES, GRANT OPTION FROM %s@'%%'", (user,))
                cursor.execute(f"GRANT {privileges} ON `{database}`.* TO %s@'%%'", (user,))
    finally:
        connection.close()
    from mysql_storage.upgrades import main as migrate
    migrate()


if __name__ == '__main__':
    main()
