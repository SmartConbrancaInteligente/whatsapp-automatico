import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional
import logging

logger = logging.getLogger(__name__)

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


class DatabaseRepository:
    def __init__(self, database_url: str = "", db_path: str = "controle.db") -> None:
        self.database_url = database_url
        self.db_path = db_path
        self.is_postgres = False
        
        # Determine if using PostgreSQL or SQLite
        if database_url and database_url.startswith("postgresql://"):
            if not HAS_PSYCOPG2:
                logger.warning("psycopg2 not available, falling back to SQLite")
                self.is_postgres = False
            else:
                self.is_postgres = True
                logger.info("Using PostgreSQL database")
        else:
            logger.info("Using SQLite database")

    @contextmanager
    def _connect(self):
        if self.is_postgres:
            conn = psycopg2.connect(self.database_url)
            try:
                yield conn
            finally:
                conn.close()
        else:
            conn = sqlite3.connect(self.db_path)
            try:
                yield conn
            finally:
                conn.close()

    @staticmethod
    def _now_br() -> datetime:
        return datetime.utcnow() - timedelta(hours=3)

    def _execute(self, cursor, query: str, params: tuple = ()) -> None:
        """Execute query with automatic placeholder conversion for PostgreSQL"""
        if self.is_postgres:
            # Convert SQLite placeholders (?) to PostgreSQL placeholders (%s)
            query = query.replace("?", "%s")
            # Handle ON CONFLICT syntax conversion for PostgreSQL
            import re
            query = re.sub(r'ON CONFLICT\(', 'ON CONFLICT (', query)
            query = query.replace("excluded.", "EXCLUDED.")
        # Decide se deve passar params ou não
        # Conta quantos placeholders existem
        has_placeholders = ("?" in query) or ("%s" in query)
        if has_placeholders:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

    def _execute_many(self, cursor, query: str, params_list: list) -> None:
        """Execute many queries with automatic placeholder conversion"""
        if self.is_postgres:
            query = query.replace("?", "%s")
        cursor.executemany(query, params_list)

    def init_schema(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            
            if self.is_postgres:
                # PostgreSQL schema
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS interacoes (
                        numero TEXT,
                        data TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pagamentos (
                        payment_id TEXT PRIMARY KEY,
                        external_reference TEXT,
                        numero TEXT,
                        status TEXT,
                        valor REAL,
                        data TEXT,
                        data_iso TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS vencimentos_override (
                        numero TEXT PRIMARY KEY,
                        vencimento TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cobrancas (
                        external_reference TEXT PRIMARY KEY,
                        numero TEXT,
                        nome TEXT,
                        valor REAL,
                        status TEXT,
                        payment_link TEXT,
                        payment_id TEXT,
                        criado_em TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS clientes_painel (
                        numero TEXT PRIMARY KEY,
                        nome TEXT,
                        login TEXT,
                        vencimento TEXT,
                        criado_em TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS numero_overrides (
                        numero_original TEXT PRIMARY KEY,
                        numero_atual TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dispatch_settings (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        habilitado INTEGER NOT NULL DEFAULT 0,
                        horario_1 TEXT NOT NULL DEFAULT '08:00',
                        horario_2 TEXT NOT NULL DEFAULT '12:00',
                        horario_3 TEXT NOT NULL DEFAULT '18:00',
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dispatch_executions (
                        id SERIAL PRIMARY KEY,
                        data TEXT NOT NULL,
                        slot TEXT NOT NULL,
                        enviados INTEGER NOT NULL DEFAULT 0,
                        executado_em TEXT,
                        UNIQUE(data, slot)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS clientes_ocultos (
                        numero TEXT PRIMARY KEY,
                        ocultado_em TEXT
                    )
                    """
                )
                
                # Check if login column exists in PostgreSQL
                cursor.execute(
                    """
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'clientes_painel' AND column_name = 'login'
                    """
                )
                if cursor.fetchone() is None:
                    try:
                        cursor.execute("ALTER TABLE clientes_painel ADD COLUMN login TEXT")
                    except Exception:
                        pass
            else:
                # SQLite schema (original)
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS interacoes (
                        numero TEXT,
                        data TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pagamentos (
                        payment_id TEXT PRIMARY KEY,
                        external_reference TEXT,
                        numero TEXT,
                        status TEXT,
                        valor REAL,
                        data TEXT,
                        data_iso TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS vencimentos_override (
                        numero TEXT PRIMARY KEY,
                        vencimento TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cobrancas (
                        external_reference TEXT PRIMARY KEY,
                        numero TEXT,
                        nome TEXT,
                        valor REAL,
                        status TEXT,
                        payment_link TEXT,
                        payment_id TEXT,
                        criado_em TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS clientes_painel (
                        numero TEXT PRIMARY KEY,
                        nome TEXT,
                        login TEXT,
                        vencimento TEXT,
                        criado_em TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS numero_overrides (
                        numero_original TEXT PRIMARY KEY,
                        numero_atual TEXT,
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dispatch_settings (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        habilitado INTEGER NOT NULL DEFAULT 0,
                        horario_1 TEXT NOT NULL DEFAULT '08:00',
                        horario_2 TEXT NOT NULL DEFAULT '12:00',
                        horario_3 TEXT NOT NULL DEFAULT '18:00',
                        atualizado_em TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dispatch_executions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data TEXT NOT NULL,
                        slot TEXT NOT NULL,
                        enviados INTEGER NOT NULL DEFAULT 0,
                        executado_em TEXT,
                        UNIQUE(data, slot)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS clientes_ocultos (
                        numero TEXT PRIMARY KEY,
                        ocultado_em TEXT
                    )
                    """
                )
                cursor.execute("PRAGMA table_info(clientes_painel)")
                panel_columns = {str(row[1]) for row in cursor.fetchall()}
                if "login" not in panel_columns:
                    cursor.execute("ALTER TABLE clientes_painel ADD COLUMN login TEXT")
            
            conn.commit()

    def register_interaction(self, number: str) -> None:
        if not number:
            return

        today = self._now_br().strftime("%d/%m/%Y")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                "SELECT 1 FROM interacoes WHERE numero = ? AND data = ?",
                (number, today),
            )
            if cursor.fetchone() is None:
                self._execute(cursor,
                    "INSERT INTO interacoes (numero, data) VALUES (?, ?)",
                    (number, today),
                )
                conn.commit()

    def has_interacted_today(self, number: str) -> bool:
        if not number:
            return False

        today = self._now_br().strftime("%d/%m/%Y")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                "SELECT 1 FROM interacoes WHERE numero = ? AND data = ?",
                (number, today),
            )
            return cursor.fetchone() is not None

    def save_payment(
        self,
        payment_id: str,
        external_reference: str,
        number: str,
        status: str,
        amount: float,
    ) -> None:
        timestamp_br = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        timestamp_iso = self._now_br().strftime("%Y-%m-%d %H:%M:%S")

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO pagamentos (payment_id, external_reference, numero, status, valor, data, data_iso)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(payment_id) DO UPDATE SET
                    external_reference = excluded.external_reference,
                    numero = excluded.numero,
                    status = excluded.status,
                    valor = excluded.valor,
                    data = excluded.data,
                    data_iso = excluded.data_iso
                """,
                (payment_id, external_reference, number, status, float(amount or 0), timestamp_br, timestamp_iso),
            )
            conn.commit()

    def get_payment(self, payment_id: str) -> Optional[Dict[str, str]]:
        if not payment_id:
            return None

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                "SELECT payment_id, external_reference, numero, status, valor, data FROM pagamentos WHERE payment_id = ?",
                (payment_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            return {
                "payment_id": str(row[0] or ""),
                "external_reference": str(row[1] or ""),
                "numero": str(row[2] or ""),
                "status": str(row[3] or ""),
                "valor": float(row[4] or 0),
                "data": str(row[5] or ""),
            }

    def get_latest_payment_status_by_number(self) -> Dict[str, str]:
        with self._connect() as conn:
            cursor = conn.cursor()
            # Usa data_iso para ordenação, que é ISO 8601 e ordena corretamente como string
            self._execute(cursor,
                '''
                SELECT p1.numero, p1.status
                FROM pagamentos p1
                INNER JOIN (
                    SELECT numero, MAX(data_iso) as max_data
                    FROM pagamentos
                    WHERE TRIM(numero) != ''
                    GROUP BY numero
                ) p2 ON p1.numero = p2.numero AND p1.data_iso = p2.max_data
                ''',
            )
            rows = cursor.fetchall()
            return {str(number): str(status) for number, status in rows}

    def get_latest_payment_details_by_number(self) -> Dict[str, Dict[str, float]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                SELECT p.numero, p.status, p.valor
                FROM pagamentos p
                JOIN (
                    SELECT numero, MAX(payment_id) AS max_payment_id
                    FROM pagamentos
                    WHERE TRIM(numero) != ''
                    GROUP BY numero
                ) latest ON p.payment_id = latest.max_payment_id
                """
            )
            rows = cursor.fetchall()
            result: Dict[str, Dict[str, float]] = {}
            for number, status, amount in rows:
                result[str(number)] = {
                    "status": str(status),
                    "amount": float(amount or 0),
                }
            return result

    def upsert_due_date_override(self, number: str, due_date: str) -> None:
        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO vencimentos_override (numero, vencimento, atualizado_em)
                VALUES (?, ?, ?)
                ON CONFLICT(numero) DO UPDATE SET
                    vencimento = excluded.vencimento,
                    atualizado_em = excluded.atualizado_em
                """,
                (number, due_date, timestamp),
            )
            conn.commit()

    def get_due_date_overrides(self) -> Dict[str, str]:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "SELECT numero, vencimento FROM vencimentos_override")
            rows = cursor.fetchall()
            return {str(number): str(due_date) for number, due_date in rows}

    def upsert_number_override(self, original_number: str, current_number: str) -> None:
        if not original_number or not current_number:
            return

        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO numero_overrides (numero_original, numero_atual, atualizado_em)
                VALUES (?, ?, ?)
                ON CONFLICT(numero_original) DO UPDATE SET
                    numero_atual = excluded.numero_atual,
                    atualizado_em = excluded.atualizado_em
                """,
                (original_number, current_number, timestamp),
            )
            conn.commit()

    def get_number_overrides(self) -> Dict[str, str]:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                SELECT numero_original, numero_atual
                FROM numero_overrides
                WHERE TRIM(numero_original) != '' AND TRIM(numero_atual) != ''
                """
            )
            rows = cursor.fetchall()
            return {str(original): str(current) for original, current in rows}

    def upsert_panel_client(self, number: str, name: str, due_date: str, login: str = "") -> None:
        if not number:
            return

        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO clientes_painel (numero, nome, login, vencimento, criado_em, atualizado_em)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(numero) DO UPDATE SET
                    nome = excluded.nome,
                    login = excluded.login,
                    vencimento = excluded.vencimento,
                    atualizado_em = excluded.atualizado_em
                """,
                (number, name, login, due_date, timestamp, timestamp),
            )
            self._execute(cursor, "DELETE FROM clientes_ocultos WHERE numero = ?", (number,))
            conn.commit()

    def get_panel_clients(self) -> List[Dict[str, str]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            if self.is_postgres:
                self._execute(cursor,
                    """
                    SELECT numero, nome, COALESCE(login, ''), vencimento, criado_em, atualizado_em
                    FROM clientes_painel
                    ORDER BY nome ASC, numero ASC
                    """
                )
            else:
                self._execute(cursor,
                    """
                    SELECT numero, nome, COALESCE(login, ''), vencimento, criado_em, atualizado_em
                    FROM clientes_painel
                    ORDER BY nome COLLATE NOCASE ASC, numero ASC
                    """
                )
            rows = cursor.fetchall()
            return [
                {
                    "numero": str(row[0] or ""),
                    "nome": str(row[1] or ""),
                    "login": str(row[2] or ""),
                    "vencimento": str(row[3] or ""),
                    "criado_em": str(row[4] or ""),
                    "atualizado_em": str(row[5] or ""),
                }
                for row in rows
            ]

    def get_panel_client(self, number: str) -> Optional[Dict[str, str]]:
        if not number:
            return None

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                "SELECT numero, nome, COALESCE(login, ''), vencimento, criado_em, atualizado_em FROM clientes_painel WHERE numero = ?",
                (number,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "numero": str(row[0] or ""),
                "nome": str(row[1] or ""),
                "login": str(row[2] or ""),
                "vencimento": str(row[3] or ""),
                "criado_em": str(row[4] or ""),
                "atualizado_em": str(row[5] or ""),
            }

    def get_dispatch_settings(self) -> Dict[str, Any]:
        defaults: Dict[str, Any] = {
            "habilitado": False,
            "horario_1": "08:00",
            "horario_2": "12:00",
            "horario_3": "18:00",
        }
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "SELECT habilitado, horario_1, horario_2, horario_3 FROM dispatch_settings WHERE id = 1")
            row = cursor.fetchone()
            if not row:
                return defaults

            return {
                "habilitado": bool(row[0]),
                "horario_1": str(row[1] or defaults["horario_1"]),
                "horario_2": str(row[2] or defaults["horario_2"]),
                "horario_3": str(row[3] or defaults["horario_3"]),
            }

    def save_dispatch_settings(self, enabled: bool, time_1: str, time_2: str, time_3: str) -> None:
        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO dispatch_settings (id, habilitado, horario_1, horario_2, horario_3, atualizado_em)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    habilitado = excluded.habilitado,
                    horario_1 = excluded.horario_1,
                    horario_2 = excluded.horario_2,
                    horario_3 = excluded.horario_3,
                    atualizado_em = excluded.atualizado_em
                """,
                (1 if enabled else 0, time_1, time_2, time_3, timestamp),
            )
            conn.commit()

    def get_recent_dispatch_executions(self, limit: int = 30) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                SELECT data, slot, enviados, executado_em
                FROM dispatch_executions
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = cursor.fetchall()
            return [
                {
                    "data": str(row[0] or ""),
                    "slot": str(row[1] or ""),
                    "enviados": int(row[2] or 0),
                    "executado_em": str(row[3] or ""),
                }
                for row in rows
            ]

    def was_dispatch_executed(self, date_str: str, slot: str) -> bool:
        if not date_str or not slot:
            return False

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                "SELECT 1 FROM dispatch_executions WHERE data = ? AND slot = ?",
                (date_str, slot),
            )
            return cursor.fetchone() is not None

    def record_dispatch_execution(self, date_str: str, slot: str, sent_count: int) -> None:
        if not date_str or not slot:
            return

        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO dispatch_executions (data, slot, enviados, executado_em)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(data, slot) DO UPDATE SET
                    enviados = excluded.enviados,
                    executado_em = excluded.executado_em
                """,
                (date_str, slot, int(sent_count or 0), timestamp),
            )
            conn.commit()

    def delete_panel_client(self, number: str) -> None:
        if not number:
            return

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "DELETE FROM clientes_painel WHERE numero = ?", (number,))
            conn.commit()

    def hide_client(self, number: str) -> None:
        if not number:
            return

        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO clientes_ocultos (numero, ocultado_em)
                VALUES (?, ?)
                ON CONFLICT(numero) DO UPDATE SET
                    ocultado_em = excluded.ocultado_em
                """,
                (number, timestamp),
            )
            conn.commit()

    def unhide_client(self, number: str) -> None:
        if not number:
            return

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "DELETE FROM clientes_ocultos WHERE numero = ?", (number,))
            conn.commit()

    def get_hidden_client_numbers(self) -> set[str]:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "SELECT numero FROM clientes_ocultos", ())
            rows = cursor.fetchall()
            return {str(row[0] or "") for row in rows}

    def upsert_charge(
        self,
        external_reference: str,
        number: str,
        name: str,
        amount: float,
        status: str,
        payment_link: str,
    ) -> None:
        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                INSERT INTO cobrancas (
                    external_reference,
                    numero,
                    nome,
                    valor,
                    status,
                    payment_link,
                    criado_em,
                    atualizado_em
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_reference) DO UPDATE SET
                    numero = excluded.numero,
                    nome = excluded.nome,
                    valor = excluded.valor,
                    status = excluded.status,
                    payment_link = excluded.payment_link,
                    atualizado_em = excluded.atualizado_em
                """,
                (
                    external_reference,
                    number,
                    name,
                    float(amount or 0),
                    status,
                    payment_link,
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()

    def get_latest_charge_by_number(self, number: str) -> Optional[Dict[str, str]]:
        if not number:
            return None

        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                SELECT external_reference, numero, nome, valor, status, payment_link, payment_id, criado_em, atualizado_em
                FROM cobrancas
                WHERE numero = ?
                ORDER BY atualizado_em DESC
                LIMIT 1
                """,
                (number,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            return {
                "external_reference": str(row[0] or ""),
                "numero": str(row[1] or ""),
                "nome": str(row[2] or ""),
                "valor": float(row[3] or 0),
                "status": str(row[4] or ""),
                "payment_link": str(row[5] or ""),
                "payment_id": str(row[6] or ""),
                "criado_em": str(row[7] or ""),
                "atualizado_em": str(row[8] or ""),
            }

    def get_number_by_external_reference(self, external_reference: str) -> str:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                "SELECT numero FROM cobrancas WHERE external_reference = ?",
                (external_reference,),
            )
            row = cursor.fetchone()
            if not row:
                return ""
            return str(row[0] or "")

    def update_charge_status(self, external_reference: str, status: str, payment_id: str) -> None:
        timestamp = self._now_br().strftime("%d/%m/%Y %H:%M:%S")
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                UPDATE cobrancas
                SET status = ?, payment_id = ?, atualizado_em = ?
                WHERE external_reference = ?
                """,
                (status, payment_id, timestamp, external_reference),
            )
            conn.commit()

    def get_all_charges(self) -> list:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._execute(cursor,
                """
                SELECT external_reference, numero, nome, valor, status, payment_link, criado_em, atualizado_em
                FROM cobrancas
                ORDER BY criado_em DESC
                """
            )
            rows = cursor.fetchall()
            result = []
            for row in rows:
                result.append({
                    "external_reference": str(row[0]),
                    "codigo": str(row[0])[:8].upper(),
                    "numero": str(row[1]),
                    "nome": str(row[2]),
                    "valor": float(row[3]),
                    "status": str(row[4]),
                    "payment_link": str(row[5]),
                    "criado_em": str(row[6]),
                    "atualizado_em": str(row[7]),
                })
            return result
