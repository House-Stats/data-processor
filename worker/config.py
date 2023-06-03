import os

from dotenv import load_dotenv

load_dotenv()

def manage_sensitive(name, default: str | None = None) -> str | None:
    v1 = os.environ.get(name)

    secret_fpath = f'/run/secrets/{name}'
    existence = os.path.exists(secret_fpath)

    if v1 is not None:
        return v1

    if existence:
        v2 = open(secret_fpath).read().rstrip('\n')
        return v2

    if all([v1 is None, not existence]) and default is None:
        raise KeyError(f'{name} environment variable is not defined')
    elif default is not None:
        return default

class Config:
    SQL_USER = manage_sensitive("POSTGRES_USER")
    SQL_PASSWORD = manage_sensitive("POSTGRES_PASSWORD")
    SQL_HOST = manage_sensitive("POSTGRES_HOST")
    MONGO_HOST = manage_sensitive("MONGO_HOST")
    MONGO_USER = manage_sensitive("MONGO_USERNAME")
    MONGO_PASSWORD = manage_sensitive("MONGO_PASSWORD")
    CELERY_BROKER_URL = manage_sensitive("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = manage_sensitive("CELERY_RESULT_BACKEND")
