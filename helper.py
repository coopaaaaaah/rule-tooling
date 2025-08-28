from config import ENV_MAP
import psycopg2
import sys


def get_db_connection(env='stg'):
    if env not in ENV_MAP.keys():
        raise ValueError(f"Invalid environment: {env}, available environments: {ENV_MAP.keys()}")

    env_config = ENV_MAP[env]

    """Create and return a PostgreSQL database connection."""
    try:
        # You can either set these as environment variables or modify directly
        connection = psycopg2.connect(
            host=env_config['DB_HOST'],
            port=env_config['DB_PORT'],
            database=env_config['DB_NAME'],
            user=env_config['DB_USER'],
            password=env_config['DB_PASSWORD']
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)