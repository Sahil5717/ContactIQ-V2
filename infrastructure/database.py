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

            CREATE TABLE IF NOT EXISTS project_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT UNIQUE NOT NULL,
                config_value TEXT NOT NULL,
                set_by INTEGER,
                set_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (set_by) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS mode_change_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                old_mode TEXT NOT NULL,
                new_mode TEXT NOT NULL,
                changed_by INTEGER,
                changed_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (changed_by) REFERENCES users(id)
            );

            -- CR-FIX-AUDIT: Override audit trail
            CREATE TABLE IF NOT EXISTS override_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                override_key TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                action TEXT NOT NULL DEFAULT 'set',
                changed_by INTEGER,
                changed_at TEXT DEFAULT (datetime('now')),
                reason TEXT,
                FOREIGN KEY (changed_by) REFERENCES users(id)
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
            CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_category ON uploaded_files(file_category);
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_active ON uploaded_files(is_active);
        """)

        # Ensure project_mode default exists
        mode_row = db.execute(
            "SELECT config_value FROM project_config WHERE config_key = 'project_mode'"
        ).fetchone()
        if not mode_row:
            db.execute(
                "INSERT INTO project_config (config_key, config_value) VALUES ('project_mode', 'opportunity')"
            )

        # Create default accounts if no users exist
        # Role mapping: admin=GDS, supervisor=EY US, analyst=Client
        count = db.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        if count == 0:
            create_user(db, 'admin', 'admin123', 'admin', 'EY GDS Admin')
            create_user(db, 'supervisor', 'super123', 'supervisor', 'EY US Consultant')
            create_user(db, 'analyst', 'analyst123', 'analyst', 'Project Analyst')
            create_user(db, 'client', 'client123', 'client', 'Client Viewer')
            # F-17 fix: Do NOT log credentials to stdout — visible in Railway deployment logs


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

def save_overrides(overrides_dict, user_id=None, reason=None):
    """Persist all overrides to DB with audit trail."""
    with get_db() as db:
        # CR-FIX-AUDIT: Load existing overrides for audit comparison
        existing = {}
        for r in db.execute("SELECT override_key, override_value FROM overrides").fetchall():
            try:
                existing[r['override_key']] = json.loads(r['override_value'])
            except (json.JSONDecodeError, TypeError):
                existing[r['override_key']] = r['override_value']

        db.execute("DELETE FROM overrides")
        for key, value in overrides_dict.items():
            db.execute(
                "INSERT INTO overrides (override_key, override_value, set_by) VALUES (?, ?, ?)",
                (key, json.dumps(value), user_id)
            )
            # Log change if value differs from previous
            old_val = existing.get(key)
            if old_val != value:
                db.execute(
                    "INSERT INTO override_audit_log (override_key, old_value, new_value, action, changed_by, reason) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (key, json.dumps(old_val) if old_val is not None else None,
                     json.dumps(value), 'set', user_id, reason)
                )
        # Log removals
        for key in existing:
            if key not in overrides_dict:
                db.execute(
                    "INSERT INTO override_audit_log (override_key, old_value, new_value, action, changed_by, reason) "
                    "VALUES (?, ?, NULL, ?, ?, ?)",
                    (key, json.dumps(existing[key]), 'remove', user_id, reason)
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


# ── Project Mode (V6: Dual-Mode Architecture) ────────────────

def get_project_mode():
    """Get current project mode: 'opportunity' or 'delivery'."""
    with get_db() as db:
        row = db.execute(
            "SELECT config_value FROM project_config WHERE config_key = 'project_mode'"
        ).fetchone()
        return row['config_value'] if row else 'opportunity'


def set_project_mode(new_mode, user_id=None):
    """Set project mode and log the change. Returns old mode."""
    if new_mode not in ('opportunity', 'delivery'):
        raise ValueError(f"Invalid mode: {new_mode}")
    old_mode = get_project_mode()
    if old_mode == new_mode:
        return old_mode
    with get_db() as db:
        db.execute(
            "UPDATE project_config SET config_value = ?, set_by = ?, set_at = datetime('now') WHERE config_key = 'project_mode'",
            (new_mode, user_id)
        )
        db.execute(
            "INSERT INTO mode_change_log (old_mode, new_mode, changed_by) VALUES (?, ?, ?)",
            (old_mode, new_mode, user_id)
        )
    return old_mode


def get_mode_change_log():
    """Get audit trail of mode changes."""
    with get_db() as db:
        rows = db.execute("""
            SELECT m.*, u.display_name as changed_by_name 
            FROM mode_change_log m 
            LEFT JOIN users u ON m.changed_by = u.id
            ORDER BY m.changed_at DESC LIMIT 20
        """).fetchall()
        return [dict(r) for r in rows]


def get_override_audit_log(limit=100):
    """CR-FIX-AUDIT: Get override change audit trail."""
    with get_db() as db:
        try:
            rows = db.execute("""
                SELECT o.*, u.username, u.display_name
                FROM override_audit_log o
                LEFT JOIN users u ON o.changed_by = u.id
                ORDER BY o.changed_at DESC LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []


# ── CR-FIX-GOV v2: Full calculation rights + override ceilings + approval workflow ──
CALCULATION_RIGHTS = {
    'admin': {
        'can_override': ['any'],
        'can_edit': ['initiative_library', 'benchmark_logic', 'formulas', 'defaults', 'model_versions', 'access'],
        'can_approve': True,
        'can_publish': True,
        'can_view': ['full_audit_trail', 'model_internals', 'raw_assumptions', 'client_output'],
    },
    'supervisor': {
        'can_override': ['adoption', 'ramp', 'benchmark', 'initiative_enabled', 'phasing', 'costAssumptions', 'volumeBasis'],
        'can_edit': ['initiative_enabled', 'adoption', 'ramp', 'scenarios', 'narrative', 'consultantNote'],
        'can_approve': True,
        'can_publish': False,
        'can_view': ['audit_trail', 'assumptions', 'confidence_bands', 'evidence_cards', 'client_output'],
        'guardrails': {
            'adoption': {'min': 0.2, 'max': 0.95, 'reason_required': True},
            'ramp': {'min': 2, 'max': 36, 'reason_required': False},
            'impact': {'min': 0.05, 'max': 0.50, 'reason_required': True},
            'benchmark': {'deviation_max_pct': 50, 'reason_required': True},
        },
    },
    'analyst': {
        'can_override': [],
        'can_edit': ['data_uploads', 'field_mapping', 'notes', 'data_corrections'],
        'can_approve': False,
        'can_publish': False,
        'can_view': ['actual_vs_derived', 'data_quality', 'anomalies', 'validation_issues'],
    },
    'client': {
        'can_override': [],
        'can_edit': [],
        'can_approve': False,
        'can_publish': False,
        'can_view': ['approved_output', 'scenarios_approved', 'roadmap'],
        'restrictions': ['no_raw_assumptions', 'no_model_internals', 'no_initiative_scoring'],
    },
}

# ── CR-FIX-K: Override rules with mandatory reason ──
OVERRIDE_RULES = {
    'adoption': {'min': 0.2, 'max': 0.95, 'roles': ['admin', 'supervisor'], 'reason_required': True},
    'impact': {'min': 0.05, 'max': 0.50, 'roles': ['admin'], 'reason_required': True},
    'ramp': {'min': 2, 'max': 36, 'roles': ['admin', 'supervisor'], 'reason_required': False},
    'benchmark': {'min': None, 'max': None, 'roles': ['admin', 'supervisor'], 'reason_required': True},
    'initiative_enabled': {'roles': ['admin', 'supervisor'], 'reason_required': False},
    'volumeBasis': {'roles': ['admin', 'supervisor'], 'reason_required': True},
    'costAssumptions': {'roles': ['admin', 'supervisor'], 'reason_required': True},
}

def check_calculation_rights(role, action, field=None):
    """CR-FIX-GOV v2: Check if a role can perform an action on a field."""
    rights = CALCULATION_RIGHTS.get(role, CALCULATION_RIGHTS.get('client', {}))
    if action == 'override':
        allowed = rights.get('can_override', [])
        if 'any' in allowed:
            return True, None
        if field and field in allowed:
            rule = OVERRIDE_RULES.get(field, {})
            return True, {'guardrails': rights.get('guardrails', {}).get(field),
                         'reason_required': rule.get('reason_required', False)}
        return False, f'Role {role} cannot override {field}'
    elif action == 'edit':
        return field in rights.get('can_edit', []), None
    elif action == 'view':
        return field in rights.get('can_view', []), None
    elif action == 'approve':
        return rights.get('can_approve', False), None
    elif action == 'publish':
        return rights.get('can_publish', False), None
    return False, 'Unknown action'

def check_override_value(field, value, role='supervisor'):
    """CR-FIX-K: Validate override value against rules and guardrails."""
    rule = OVERRIDE_RULES.get(field)
    if not rule:
        return True, None
    if role not in rule.get('roles', []):
        return False, f'Role {role} not authorized for {field} override'
    if rule.get('min') is not None and value < rule['min']:
        return False, f'{field} below minimum ({rule["min"]})'
    if rule.get('max') is not None and value > rule['max']:
        return False, f'{field} above maximum ({rule["max"]})'
    return True, rule.get('reason_required', False)

# ── CR-FIX-AG: Model versioning ──
MODEL_VERSION = '8.0.0'

def get_model_version():
    return MODEL_VERSION

def build_version_stamp(data=None, waterfall=None):
    """Build a version stamp for output payloads."""
    import datetime
    return {
        'modelVersion': MODEL_VERSION,
        'dataTimestamp': datetime.datetime.utcnow().isoformat() + 'Z',
        'volumeBasis': data.get('volumeScaling', {}).get('activeBasis', 'source') if data else 'unknown',
        'dataQuality': waterfall.get('dataQuality', {}).get('label', 'Unknown') if waterfall else 'Unknown',
    }
