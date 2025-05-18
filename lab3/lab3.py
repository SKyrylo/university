# from sqlalchemy import create_engine, text
# import pandas as pd
# from io import StringIO
#
# pd.options.display.max_columns=None
# pd.options.mode.chained_assignment = None
#
# CSV_PATH = "weather.csv"
# DB_URL = "postgresql+psycopg2://postgres:16022005@localhost/Lab3"
# TABLE = "weather"
#
# # читаем только нужные колонки (если вдруг csv всё-таки большой)
# cols = [
#     "country", "wind_degree", "wind_kph", "wind_direction",
#     "last_updated", "sunrise", "precip_mm"
# ]
# df = pd.read_csv(CSV_PATH, usecols=cols)
# df = df[cols]
#
# engine = create_engine(DB_URL)
#
# with engine.begin() as conn:          # автокоммит + rollback при ошибке
#     # ---> быстрое BULK-копирование
#     buf = StringIO()
#     df.to_csv(buf, index=False, header=False)
#     buf.seek(0)
#     # важен порядок колонок!
#     copy_sql = f"COPY {TABLE} ({', '.join(cols)}) FROM STDIN WITH (FORMAT CSV)"
#     conn.connection.cursor().copy_expert(copy_sql, buf)
#
# print("Импорт завершён, строк:", len(df))
# -------------------------------------------------------
# from sqlalchemy import create_engine, text
#
# DB_URL = "postgresql+psycopg2://postgres:16022005@localhost/Lab3"
#
# engine = create_engine(DB_URL)
#
# rule_sql = """
# UPDATE weather
# SET    go_out = CASE
#           WHEN wind_kph <= 10
#            AND precip_mm <= 0.25
#           THEN TRUE
#           ELSE FALSE
#        END
# """
#
# with engine.begin() as conn:   # автоматически commit / rollback
#     conn.execute(text(rule_sql))
#
# print("Колонка is_good_to_go заполнена")
# -----------------------------------------------------
# """
# Запрос строк из таблицы weather по указанным country и last_updated.
#
# ▪ country       – строка (например: Ukraine)
# ▪ last_updated  – дата-время в формате YYYY-MM-DD или YYYY-MM-DD HH:MM
#
# Для вывода используем библиотеку tabulate (pip install tabulate).
# """
#
# from sqlalchemy import create_engine, text
# from tabulate import tabulate
#
# # 1) строка подключения  ── поменяйте при необходимости
# DB_URL = "postgresql+psycopg2://postgres:16022005@localhost/Lab3"
#
# engine = create_engine(DB_URL)
#
# def ask_country() -> str:
#     return input("Country: ").strip()
#
# def ask_timestamp() -> str:
#     return input("Last updated (YYYY-MM-DD or YYYY-MM-DD HH:MM): ").strip()
#
# def fetch_rows(country: str, date: str):
#     if len(date) == 16:
#         sql = text(
#             """
#             SELECT id, country, wind_kph, wind_direction,
#                    last_updated, sunrise, precip_mm, go_out
#             FROM   weather
#             WHERE  country = :country
#               AND  last_updated = :date
#             ORDER  BY last_updated
#             """
#         )
#     else: # len(date) == 10
#         sql = text(
#             """
#             SELECT id, country, wind_kph, wind_direction,
#                    last_updated, sunrise, precip_mm, go_out
#             FROM   weather
#             WHERE  country = :country
#               AND  starts_with(last_updated, :date)
#             ORDER  BY last_updated
#             """
#         )
#
#     with engine.connect() as conn:
#         return conn.execute(sql, {"country": country, "date": date}).fetchall()
#
# def main():
#     try:
#         country = ask_country()
#         ts      = ask_timestamp()
#     except ValueError as e:
#         print(e)
#         return
#
#     rows = fetch_rows(country, ts)
#     if not rows:
#         print("Ничего не найдено 🫤")
#         return
#
#     headers = [
#         "id", "country", "wind_kph", "wind_dir",
#         "last_updated", "sunrise", "precip_mm", "good_to_go?"
#     ]
#     print(tabulate(rows, headers=headers, tablefmt="grid"))
#
# if __name__ == "__main__":
#     main()
# ------------------------------------------------
"""
PostgreSQL → MySQL one-way migration for the `weather` table.

▪ Requires:  SQLAlchemy ≥ 2.0, mysql-connector-python, psycopg2-binary
▪ Usage:     python migrate_pg_to_mysql.py
"""

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean,
    create_engine, MetaData
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# 1. CONNECTION STRINGS – change host / user / password if needed
PG_URL  = "postgresql+psycopg2://postgres:16022005@localhost/Lab3"
MY_URL  = "mysql+mysqlconnector://weather_user:SecretPass456@127.0.0.1:3306/lab3_db?charset=utf8mb4"


# 2. ORM model (must exactly match the source table)
Base = declarative_base()


class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer(), primary_key=True)
    country = Column(String(50))
    wind_degree = Column(Integer())
    wind_kph = Column(Float())
    wind_direction = Column(String(3))
    last_updated = Column(String(16))
    sunrise = Column(String(8))
    precip_mm = Column(Float())
    go_out = Column(Boolean)


# 3. PREPARE ENGINES & CREATE TARGET SCHEMA
pg_engine = create_engine(PG_URL)
my_engine = create_engine(MY_URL)

print("⏳  Creating schema in MySQL …")
Base.metadata.create_all(my_engine)
print("✅  Schema ready")

# 4. DATA COPY IN BATCHES
BATCH = 10_000
offset = 0
total  = 0
cols   = Weather.__table__.columns.keys()

with Session(pg_engine) as pg_sess, Session(my_engine) as my_sess:
    while True:
        # читаем порцию из PostgreSQL
        rows = (
            pg_sess.query(Weather)
                   .order_by(Weather.id)
                   .offset(offset)
                   .limit(BATCH)
                   .all()
        )
        if not rows:
            break

        # создаём "чистые" объекты для MySQL
        objects = [
            Weather(**{c: getattr(r, c) for c in cols})
            for r in rows
        ]

        my_sess.bulk_save_objects(objects, return_defaults=False)
        my_sess.commit()

        offset += len(rows)
        total  += len(rows)
        print(f"→ copied {total:,} rows …")

