"""Run only the isolated Docker schema migration from the host interpreter."""
import argparse
import os
import sys
from pathlib import Path

from sqlalchemy.engine import make_url


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--private-env', type=Path, required=True)
    args = parser.parse_args()
    values = dict(line.split('=',1) for line in args.private_env.read_text(encoding='utf-8-sig').splitlines() if '=' in line and not line.startswith('#'))
    target = make_url(values['RADIO_DOCKER_MIGRATION_DATABASE_URL']).set(host='127.0.0.1', port=int(values.get('MYSQL_DOCKER_PORT','13307')))
    if target.port != 13307 or target.database != values.get('MYSQL_DATABASE','radio_acceptance'):
        raise SystemExit('Refusing an unexpected host migration target')
    os.environ['DATABASE_URL'] = target.render_as_string(hide_password=False)
    os.environ['BUSINESS_DATABASE_URL'] = ''
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backend'))
    from mysql_storage.upgrades import main as migrate
    migrate()


if __name__ == '__main__':
    main()
