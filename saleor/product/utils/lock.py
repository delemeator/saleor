from contextlib import contextmanager

from django.db import connection


@contextmanager
def pg_try_advisory_lock(lock_id: int):
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", [lock_id])
        acquired = cursor.fetchone()[0]

    if not acquired:
        yield False
        return

    try:
        yield True
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", [lock_id])
