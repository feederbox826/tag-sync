import sqlite3
from datetime import datetime

db = sqlite3.connect('stash-tags.db')
cursor = db.cursor()

def setup_sqlite():
    # set up migrations
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stash_tags (
        local_id TEXT NOT NULL PRIMARY KEY,
        stashdb_id TEXT NOT NULL,
        check_time DATE DEFAULT NULL,
        ignore INT DEFAULT 0
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stash_tags_errors (
        local_id TEXT NOT NULL PRIMARY KEY,
        name TEXT DEFAULT NULL,
        missing BOOLEAN DEFAULT FALSE
    );""")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS local_id_index ON stash_tags (local_id);")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS stashdb_id_index ON stash_tags (stashdb_id);")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS local_id_index ON stash_tags_errors (local_id);")
    db.commit()

def migrate():
    setup_sqlite()

def add_ids(local_id, stashdb_id):
    cursor.execute("INSERT INTO stash_tags VALUES (?, ?, NULL, FALSE)", (local_id, stashdb_id))
    db.commit()

def check_id(local_id):
    now = str(datetime.now())
    cursor.execute("UPDATE stash_tags SET check_time = ? WHERE local_id = ?", [now, local_id])
    db.commit()

def lookup_localid(local_id):
    cursor.execute("SELECT * FROM stash_tags WHERE local_id = ?", [local_id])
    return cursor.fetchone()

def get_unchecked():
    cursor.execute("SELECT * FROM stash_tags WHERE check_time IS NULL")
    return cursor.fetchall()

def add_error(local_id, missing, name):
    cursor.execute("INSERT INTO stash_tags_errors VALUES (?, ?, ?)", (local_id, missing, name))
    db.commit()

def error_add_name(local_id, name):
    cursor.execute("UPDATE stash_tags_errors SET name = ? WHERE local_id = ?", [name, local_id])
    db.commit()

def lookup_error(local_id):
    cursor.execute("SELECT * FROM stash_tags_errors WHERE local_id = ?", [local_id])
    return cursor.fetchone()

def getall_errors():
    cursor.execute("SELECT * FROM stash_tags_errors")
    return cursor.fetchall()

def remove_error(local_id):
    cursor.execute("DELETE FROM stash_tags_errors WHERE local_id = ?", [local_id])
    db.commit()