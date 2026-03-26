import sqlite3
from pathlib import Path
from datetime import datetime
import pytz

DB_PATH = Path("/app/data/progresso.db")
TZ_SP   = pytz.timezone('America/Sao_Paulo')

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS progresso (
                usuario          TEXT PRIMARY KEY,
                ultimo_id        TEXT,
                ultimo_titulo    TEXT,
                atualizado_em    TEXT
            )
        """)

def gravar_progresso(usuario: str, id_noticia: str, titulo: str):
    agora = datetime.now(TZ_SP).strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO progresso (usuario, ultimo_id, ultimo_titulo, atualizado_em)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(usuario) DO UPDATE SET
                ultimo_id     = excluded.ultimo_id,
                ultimo_titulo = excluded.ultimo_titulo,
                atualizado_em = excluded.atualizado_em
        """, (usuario, id_noticia, titulo, agora))

def ler_progresso():
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT usuario, ultimo_id, ultimo_titulo, atualizado_em FROM progresso ORDER BY atualizado_em DESC"
        ).fetchall()
    return [dict(r) for r in rows]