import os
import time
import threading
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------- helpers ---------- #
DB_SETTINGS = {
    "host": os.getenv("hostname"),
    "dbname": os.getenv("database"),
    "user": os.getenv("user"),
    "password": os.getenv("pwd"),
    "port": os.getenv("port_id"),
}


def get_conn():
    """Return a fresh database connection."""
    return psycopg2.connect(**DB_SETTINGS)


# ---------- setup ---------- #
def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_counter (
                user_id  INTEGER PRIMARY KEY,
                counter  INTEGER,
                version  INTEGER
            )
            """
        )
        cur.execute("SELECT 1 FROM user_counter WHERE user_id = 1")
        if cur.fetchone():
            cur.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1")
        else:
            cur.execute("INSERT INTO user_counter VALUES (1, 0, 0)")


# ---------- common ops ---------- #
def clear_counter():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("UPDATE user_counter SET counter = 0 WHERE user_id = 1")


def read_counter():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        return cur.fetchone()[0]


# ---------- concurrency demos ---------- #
def lost_update_case():
    with get_conn() as conn, conn.cursor() as cur:
        for _ in range(10_000):
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            value = cur.fetchone()[0] + 1
            cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (value,))
            conn.commit()


def in_place_case():
    with get_conn() as conn, conn.cursor() as cur:
        for _ in range(10_000):
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
            conn.commit()


def row_lock_case():
    with get_conn() as conn, conn.cursor() as cur:
        for _ in range(10_000):
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
            value = cur.fetchone()[0] + 1
            cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (value,))
            conn.commit()


def optimistic_case():
    with get_conn() as conn, conn.cursor() as cur:
        for _ in range(10_000):
            while True:
                cur.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                counter, ver = cur.fetchone()
                upd_stmt = """
                    UPDATE user_counter
                    SET counter = %s, version = %s
                    WHERE user_id = 1 AND version = %s
                """
                cur.execute(upd_stmt, (counter + 1, ver + 1, ver))
                conn.commit()
                if cur.rowcount:  # success
                    break


# ---------- runner ---------- #
def run_case(func, label):
    clear_counter()
    start = time.time()

    threads = [threading.Thread(target=func) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    elapsed = time.time() - start
    print(f"{label}: {elapsed:.2f}s, final counter = {read_counter()}")


if __name__ == "__main__":
    init_db()
    run_case(lost_update_case, "Lost Update")
    run_case(in_place_case, "In-place Update")
    run_case(row_lock_case, "Row-level Locking")
    run_case(optimistic_case, "Optimistic Concurrency")
