"""
ContactIQ — Database Layer
SQLite persistence for users, sessions, uploaded files, and overrides.
Lightweight, zero external dependencies beyond stdlib.
"""
import os
import sqlite3
import hashlib
import secrets
import json
from datetime import datetime, timedelta
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'serviceedge.db')


def _ensure_dir():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


@contextmanager
def get_db():
    """Context manager for database connections."""
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize database schema. Safe to call multiple times."""
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'supervisor',
                display_name TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                last_login TEXT,
                is_active INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                expires_at TEXT NOT NULL,
                ip_address TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS uploaded_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                original_name TEXT NOT NULL,
                file_category TEXT NOT NULL,
                file_path TEXT NOT NULL,
                uploaded_by INTEGER,
                uploaded_at TEXT DEFAULT (datetime('now')),
                is_active INTEGER DEFAULT 1,
                file_size INTEGER,
                schema_valid INTEGER DEFAULT 0,
                validation_notes TEXT,
                FOREIGN KEY (uploaded_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                override_key TEXT NOT NULL,
                override_value TEXT NOT NULL,
                set_by INTEGER,
                set_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (set_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS data_source_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL DEFAULT 'backend',
                set_by INTEGER,
                set_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (set_by) REFERENCES users(id)
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
            CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_category ON uploaded_files(file_category);
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_active ON uploaded_files(is_active);
        """)

        # Create default accounts if no users exist (P2-5: 3-tier auth)
        count = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        if count == 0:
            create_user(db, 'admin', 'admin123', 'admin', 'EY Administrator')
            create_user(db, 'supervisor', 'super123', 'supervisor', 'EY Supervisor')
            create_user(db, 'analyst', 'analyst123', 'analyst', 'EY Analyst')
            print("[DB] Default users created: admin/admin123 (admin), supervisor/super123 (supervisor), analyst/analyst123 (analyst)")


# ── User Management ──────────────────────────────────────────

def _hash_password(password, salt=None):
    """Hash password with salt using SHA-256."""
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256((salt + password).encode()).hexdigest()
    return hashed, salt


def create_user(db, username, password, role='supervisor', display_name=None):
    """Create a new user. Returns user id."""
    hashed, salt = _hash_password(password)
    db.execute(
        "INSERT INTO users (username, password_hash, salt, role, display_name) VALUES (?, ?, ?, ?, ?)",
        (username, hashed, salt, role, display_name or username)
    )
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def verify_user(username, password):
    """Verify credentials. Returns user dict or None."""
    with get_db() as db:
        user = db.execute(
            "SELECT * FROM users WHERE username = ? AND is_active = 1", (username,)
        ).fetchone()
        if not user:
            return None
        hashed, _ = _hash_password(password, user['salt'])
        if hashed != user['password_hash']:
            return None
        db.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (user['id'],))
        return dict(user)


# ── Session Management ───────────────────────────────────────

def create_session(user_id, ip_address=None, hours=24):
    """Create a session token. Returns token string."""
    token = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(hours=hours)).isoformat()
    with get_db() as db:
        db.execute(
            "INSERT INTO sessions (token, user_id, expires_at, ip_address) VALUES (?, ?, ?, ?)",
            (token, user_id, expires, ip_address)
        )
    return token


def validate_session(token):
    """Validate session token. Returns user dict or None."""
    if not token:
        return None
    with get_db() as db:
        row = db.execute("""
            SELECT u.*, s.expires_at FROM sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.token = ? AND u.is_active = 1
        """, (token,)).fetchone()
        if not row:
            return None
        if datetime.fromisoformat(row['expires_at']) < datetime.utcnow():
            db.execute("DELETE FROM sessions WHERE token = ?", (token,))
            return None
        return dict(row)


def destroy_session(token):
    """Delete a session."""
    with get_db() as db:
        db.execute("DELETE FROM sessions WHERE token = ?", (token,))


def cleanup_expired_sessions():
    """Remove expired sessions."""
    with get_db() as db:
        db.execute("DELETE FROM sessions WHERE expires_at < datetime('now')")


# ── File Management ──────────────────────────────────────────

def record_upload(filename, original_name, category, file_path, user_id, file_size, valid=False, notes=''):
    """Record a file upload. Deactivates previous uploads for same category."""
    with get_db() as db:
        # Deactivate previous uploads for this category
        db.execute(
            "UPDATE uploaded_files SET is_active = 0 WHERE file_category = ? AND is_active = 1",
            (category,)
        )
        db.execute("""
            INSERT INTO uploaded_files 
            (filename, original_name, file_category, file_path, uploaded_by, file_size, schema_valid, validation_notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (filename, original_name, category, file_path, user_id, file_size, int(valid), notes))
        return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def get_active_uploads():
    """Get all currently active (latest) uploads by category."""
    with get_db() as db:
        rows = db.execute("""
            SELECT * FROM uploaded_files 
            WHERE is_active = 1 
            ORDER BY file_category
        """).fetchall()
        return [dict(r) for r in rows]


def get_upload_by_category(category):
    """Get the active upload for a specific category."""
    with get_db() as db:
        row = db.execute(
            "SELECT * FROM uploaded_files WHERE file_category = ? AND is_active = 1",
            (category,)
        ).fetchone()
        return dict(row) if row else None


def clear_upload(category, user_id=None):
    """Deactivate upload for a category (revert to backend data)."""
    with get_db() as db:
        db.execute(
            "UPDATE uploaded_files SET is_active = 0 WHERE file_category = ? AND is_active = 1",
            (category,)
        )


# ── Data Source Config ───────────────────────────────────────

def get_data_source():
    """Get current data source setting."""
    with get_db() as db:
        row = db.execute(
            "SELECT * FROM data_source_config ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else {'source_type': 'backend'}


def set_data_source(source_type, user_id=None):
    """Set data source: 'backend' or 'upload'."""
    with get_db() as db:
        db.execute(
            "INSERT INTO data_source_config (source_type, set_by) VALUES (?, ?)",
            (source_type, user_id)
        )


# ── Override Persistence ─────────────────────────────────────

def save_overrides(overrides_dict, user_id=None):
    """Persist all overrides to DB."""
    with get_db() as db:
        db.execute("DELETE FROM overrides")
        for key, value in overrides_dict.items():
            db.execute(
                "INSERT INTO overrides (override_key, override_value, set_by) VALUES (?, ?, ?)",
                (key, json.dumps(value), user_id)
            )


def load_overrides():
    """Load all persisted overrides."""
    with get_db() as db:
        rows = db.execute("SELECT override_key, override_value FROM overrides").fetchall()
        result = {}
        for r in rows:
            try:
                result[r['override_key']] = json.loads(r['override_value'])
            except (json.JSONDecodeError, TypeError):
                result[r['override_key']] = r['override_value']
        return result
