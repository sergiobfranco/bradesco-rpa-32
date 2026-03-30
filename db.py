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

def gravar_erro(usuario: str, id_noticia: str, titulo: str, motivo: str):
    init_db()
    agora = datetime.now(TZ_SP).strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS erros (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario        TEXT,
                id_noticia     TEXT,
                titulo         TEXT,
                motivo_erro    TEXT,
                ocorrido_em    TEXT,
                UNIQUE(usuario, id_noticia)
            )
        """)
        # Grava por cima se mesmo operador + mesmo ID
        conn.execute("""
            INSERT INTO erros (usuario, id_noticia, titulo, motivo_erro, ocorrido_em)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(usuario, id_noticia) DO UPDATE SET
                titulo      = excluded.titulo,
                motivo_erro = excluded.motivo_erro,
                ocorrido_em = excluded.ocorrido_em
        """, (usuario, id_noticia, titulo, motivo, agora))

        # Mantém no máximo 100 linhas — descarta as mais antigas do mesmo operador
        conn.execute("""
            DELETE FROM erros
            WHERE usuario = ?
              AND id NOT IN (
                SELECT id FROM erros
                WHERE usuario = ?
                ORDER BY ocorrido_em DESC
                LIMIT 100
              )
        """, (usuario, usuario))


def ler_erros():
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS erros (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario        TEXT,
                id_noticia     TEXT,
                titulo         TEXT,
                motivo_erro    TEXT,
                ocorrido_em    TEXT,
                UNIQUE(usuario, id_noticia)
            )
        """)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT usuario, id_noticia, titulo, motivo_erro, ocorrido_em FROM erros ORDER BY ocorrido_em DESC"
        ).fetchall()
    return [dict(r) for r in rows]    

def get_ultimo_progresso(usuario: str):
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ultimo_id, ultimo_titulo, atualizado_em FROM progresso WHERE usuario = ?",
            (usuario,)
        ).fetchone()
    return dict(row) if row else None

def remover_erro(usuario: str, id_noticia: str):
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        try:
            conn.execute(
                "DELETE FROM erros WHERE usuario = ? AND id_noticia = ?",
                (usuario, id_noticia)
            )
        except Exception:
            pass