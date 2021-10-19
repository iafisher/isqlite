import cProfile
import sqlite3
import sys
import timeit

from isqlite import Database


def benchmark_isqlite():
    with Database(":memory:") as db:
        db.create_table("counter", ["n INTEGER NOT NULL"])

        for n in range(100000):
            db.insert("counter", {"n": n})

        db.select("counter", where="n > 1000")


def benchmark_sqlite3():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE counter(n INTEGER NOT NULL)")

        for n in range(100000):
            conn.execute("INSERT INTO counter(n) VALUES (?)", (n,))

        conn.execute("SELECT * FROM counter WHERE n > 1000")


def benchmark():
    sqlite3_results = timeit.timeit(
        "benchmark_sqlite3()", number=1, setup="from __main__ import benchmark_sqlite3"
    )
    print(f"sqlite3: {sqlite3_results:0.3f} seconds")

    isqlite_results = timeit.timeit(
        "benchmark_isqlite()", number=1, setup="from __main__ import benchmark_isqlite"
    )
    print(f"isqlite: {isqlite_results:0.3f} seconds")


def profile():
    cProfile.run("benchmark_isqlite()", sort="cumulative")


if __name__ == "__main__":
    if "--profile" in sys.argv:
        profile()
    else:
        benchmark()
