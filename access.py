"""Named local identities and revocable opaque sessions; no external identity provider."""
import hashlib
import json
import os
import re
import secrets
import time

from fastapi import HTTPException

ROLES = {"viewer", "operator", "admin"}


def password_hash(password, salt=None):
    salt = salt or secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600000).hex()
    return "pbkdf2_sha256$600000$" + salt + "$" + derived


def verify_password(password, encoded):
    try:
        algorithm, rounds, salt, expected = encoded.split("$")
        if algorithm != "pbkdf2_sha256" or int(rounds) != 600000:
            return False
        actual = password_hash(password, salt)
        return secrets.compare_digest(actual, encoded)
    except (ValueError, TypeError, AttributeError):
        return False


class AccessControl:
    def __init__(self, store, legacy_token):
        self.store, self.legacy_token = store, legacy_token
        self.users = json.loads(os.getenv("SIGNALDESK_USERS_JSON", "{}"))
        if not isinstance(self.users, dict):
            raise ValueError("SIGNALDESK_USERS_JSON must be an object")
        for name, user in self.users.items():
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", name) or not isinstance(user, dict):
                raise ValueError("Invalid user configuration")
            if user.get("role") not in ROLES or not re.fullmatch(r"pbkdf2_sha256\$600000\$[0-9a-f]{32}\$[0-9a-f]{64}", user.get("password_hash", "")):
                raise ValueError("Invalid password hash or role")
        self.dummy_hash = password_hash(secrets.token_hex(32))
        if os.getenv("SIGNALDESK_REQUIRE_AUTH", "0") == "1" and not (self.users or legacy_token):
            raise ValueError("Authentication is required but no identities are configured")

    def initialize(self):
        with self.store.transaction(True) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS auth_sessions(token_hash TEXT PRIMARY KEY, username TEXT NOT NULL, expires_at REAL NOT NULL, config_hash TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS login_limits(client_hash TEXT PRIMARY KEY, window_id INTEGER NOT NULL, attempts INTEGER NOT NULL)")
            conn.execute("DELETE FROM auth_sessions WHERE expires_at<=?", (time.time(),))
            conn.execute("DELETE FROM login_limits WHERE window_id<?", (int(time.time() // 60) - 1,))

    def config_hash(self, name):
        return hashlib.sha256(json.dumps(self.users[name], sort_keys=True).encode()).hexdigest()

    def login(self, username, password, client):
        client_hash = hashlib.sha256(client.encode()).hexdigest()
        window_id = int(time.time() // 60)
        with self.store.transaction(True) as conn:
            conn.execute("DELETE FROM login_limits WHERE window_id<?", (window_id - 1,))
            conn.execute("INSERT INTO login_limits VALUES(?,?,1) ON CONFLICT(client_hash) DO UPDATE SET attempts=CASE WHEN login_limits.window_id=excluded.window_id THEN login_limits.attempts+1 ELSE 1 END,window_id=excluded.window_id", (client_hash, window_id))
            count = conn.execute("SELECT attempts FROM login_limits WHERE client_hash=?", (client_hash,)).fetchone()[0]
        if count > 10:
            raise HTTPException(429, "Too many login attempts", headers={"Retry-After": "60"})
        user = self.users.get(username)
        valid = verify_password(password, user["password_hash"] if user else self.dummy_hash)
        if not user or not valid:
            raise HTTPException(401, "Invalid credentials")
        token = secrets.token_urlsafe(32)
        expires = time.time() + 3600
        with self.store.transaction(True) as conn:
            conn.execute("DELETE FROM auth_sessions WHERE expires_at<=? OR username=?", (time.time(), username))
            conn.execute("INSERT INTO auth_sessions VALUES(?,?,?,?)", (hashlib.sha256(token.encode()).hexdigest(), username, expires, self.config_hash(username)))
        return {"access_token": token, "token_type": "bearer", "expires_in": 3600, "username": username, "role": user["role"]}

    def authorize(self, request):
        header = request.headers.get("authorization", "")
        credential = header[7:] if header.startswith("Bearer ") else ""
        principal = None
        if self.legacy_token and secrets.compare_digest(credential.encode(), self.legacy_token.encode()):
            principal = {"username": "service-token", "role": "operator"}
        elif credential and self.users:
            with self.store.transaction() as conn:
                row = conn.execute("SELECT username,expires_at,config_hash FROM auth_sessions WHERE token_hash=?", (hashlib.sha256(credential.encode()).hexdigest(),)).fetchone()
            if row and row["expires_at"] > time.time() and row["username"] in self.users:
                name = row["username"]
                if secrets.compare_digest(row["config_hash"], self.config_hash(name)):
                    principal = {"username": name, "role": self.users[name]["role"]}
        elif not self.users and not self.legacy_token:
            principal = {"username": "local-demo", "role": "admin"}
        if principal is None:
            raise HTTPException(401, "Valid bearer token required", headers={"WWW-Authenticate": "Bearer"})
        if request.method not in {"GET", "HEAD", "OPTIONS"} and principal["role"] == "viewer" and request.url.path != "/api/auth/logout":
            raise HTTPException(403, "Operator role required")
        request.state.actor = principal["username"]
        request.state.role = principal["role"]
        return principal

    def logout(self, request):
        credential = request.headers.get("authorization", "")[7:]
        with self.store.transaction(True) as conn:
            conn.execute("DELETE FROM auth_sessions WHERE token_hash=?", (hashlib.sha256(credential.encode()).hexdigest(),))


class BodyLimitMiddleware:
    """Bound streamed bodies, including requests without Content-Length."""
    def __init__(self, app, max_bytes=2_000_000):
        self.app, self.max_bytes = app, max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        messages, size = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            size += len(message.get("body", b""))
            if size > self.max_bytes:
                await send({"type": "http.response.start", "status": 413, "headers": [(b"content-type", b"application/json")]})
                await send({"type": "http.response.body", "body": b'{"detail":"Request body too large"}'})
                return
            messages.append(message)
            if not message.get("more_body", False):
                break
        async def replay():
            if messages:
                return messages.pop(0)
            return await receive()
        await self.app(scope, replay, send)
