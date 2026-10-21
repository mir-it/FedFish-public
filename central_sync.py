"""Durable, background FedFish Central uploads. Never sends orders to Synapse."""
from __future__ import annotations
import getpass
import hashlib
import json
import socket
import sqlite3
import ssl
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPSHandler, HTTPRedirectHandler, ProxyHandler

MAX_EVENT_BYTES = 2 * 1024 * 1024
PAST_ORDERS_IMPORTED = 'past_orders_imported_at'
HELLO_SECONDS = 300


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _outcome(record: dict) -> str:
    response = record.get('response')
    # FedFish caches a response without "error" only after Synapse accepted the order.
    return 'failed' if isinstance(response, dict) and response.get('error') else 'succeeded'


def _post(config: dict, path: str, body: dict) -> dict:
    url = urlparse(config['url'])
    if url.username or url.password or url.query or url.fragment:
        raise ValueError('Invalid Central base URL')
    if url.scheme != 'https' and not (url.scheme == 'http' and url.hostname in ('127.0.0.1', 'localhost')):
        raise ValueError('Central requires HTTPS except on this computer')
    context = ssl.create_default_context(cafile=config.get('ca_file'))
    opener = build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=context))
    request = Request(config['url'].rstrip('/') + path, data=json.dumps(body, ensure_ascii=False, allow_nan=False).encode('utf-8'),
                      headers={'Content-Type': 'application/json'}, method='POST')
    with opener.open(request, timeout=10) as response:
        return json.load(response)


def upload(config: dict, event: dict) -> dict:
    ack = _post(config, '/v1/events', event)
    if ack.get('event_id') != event['event_id'] or type(ack.get('sequence')) is not int or ack['sequence'] < 1:
        raise ValueError('Central acknowledgement does not match the event')
    return ack


def hello(config: dict, report: dict) -> dict:
    return _post(config, '/v1/hello', report)


class CentralSync:
    def __init__(self, base: Path, config: dict, sender=None, greeter=None, start=True):
        self.config = dict(config)
        self.sender = sender or upload
        self.greeter = greeter or hello
        self.next_hello = 0.0
        self.client_name = str(config.get('client_name') or '').strip() or socket.gethostname()
        self.db = Path(base) / '.fedfish-central' / 'outbox.sqlite3'
        self.db.parent.mkdir(parents=True, exist_ok=True)
        self.wake = threading.Event()
        self.stop_event = threading.Event()
        self.flush_lock = threading.Lock()
        self.interval = max(5, int(config.get('retry_seconds', 30)))
        with self.connect() as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('''CREATE TABLE IF NOT EXISTS outbox (
                event_id TEXT PRIMARY KEY, created_at TEXT NOT NULL, event_json TEXT NOT NULL,
                delivered_at TEXT, server_sequence INTEGER, attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT, blocked INTEGER NOT NULL DEFAULT 0)''')
            c.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        self.thread = None
        if start:
            self.thread = threading.Thread(target=self._run, name='fedfish-central-sync', daemon=True)
            self.thread.start()

    @contextmanager
    def connect(self):
        c = sqlite3.connect(self.db, timeout=0.5)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA synchronous=FULL')
        try:
            with c: yield c
        finally:
            c.close()

    def _event(self, so_num: str, record: dict, *, event_id: str, occurred_at: str | None, source: str) -> dict:
        event = {'schema_version': 1, 'event_id': event_id, 'so_num': so_num, 'occurred_at': occurred_at,
                 'source': source, 'outcome': _outcome(record), 'client_name': self.client_name,
                 'computer': socket.gethostname(), 'payload': record['payload']}
        if 'response' in record: event['response'] = record['response']
        return event

    def _insert(self, c, event: dict, stamp: str, *, ignore_existing: bool = False) -> None:
        encoded = json.dumps(event, ensure_ascii=False, allow_nan=False)
        blocked = len(encoded.encode('utf-8')) > MAX_EVENT_BYTES
        verb = 'INSERT OR IGNORE' if ignore_existing else 'INSERT'
        c.execute(verb + ' INTO outbox(event_id,created_at,event_json,blocked,last_error) VALUES (?,?,?,?,?)',
                  (event['event_id'], stamp, encoded, int(blocked), 'Event exceeds Central 2 MiB limit' if blocked else None))

    def enqueue(self, so_num: str, record: dict) -> str:
        stamp = _now()
        event = self._event(so_num, record, event_id=str(uuid.uuid4()), occurred_at=stamp, source='fedfish')
        event['actor'] = getpass.getuser()
        with self.connect() as c:
            self._insert(c, event, stamp)
        self.wake.set()
        return event['event_id']

    def import_past_orders_once(self, records: dict[str, dict]) -> int | None:
        """Queue this PC's cached orders the first time Central is set up here; later calls do nothing."""
        stamp = _now()
        with self.connect() as c:
            if c.execute('SELECT 1 FROM meta WHERE key=?', (PAST_ORDERS_IMPORTED,)).fetchone():
                return None
            for so_num, record in sorted(records.items()):
                # Same ID as server.py import-cache, so a cache imported both ways is recorded once.
                canonical = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
                event_id = 'legacy-' + hashlib.sha256((so_num + '\t' + canonical).encode('utf-8')).hexdigest()
                event = self._event(so_num, record, event_id=event_id, occurred_at=None, source='legacy-cache')
                self._insert(c, event, stamp, ignore_existing=True)
            c.execute('INSERT INTO meta(key,value) VALUES (?,?)', (PAST_ORDERS_IMPORTED, stamp))
        self.wake.set()
        return len(records)

    def flush_once(self):
        if not self.flush_lock.acquire(blocking=False): return
        try:
            while not self.stop_event.is_set():
                with self.connect() as c:
                    rows = c.execute('SELECT event_id,event_json FROM outbox WHERE delivered_at IS NULL AND blocked=0 '
                                     'ORDER BY created_at,event_id LIMIT 100').fetchall()
                if not rows: return
                for row in rows:
                    if self.stop_event.is_set(): return
                    try:
                        ack = self.sender(self.config, json.loads(row['event_json']))
                        # Check even injected senders: no deletion/acknowledgement on mismatch.
                        if ack.get('event_id') != row['event_id'] or type(ack.get('sequence')) is not int or ack['sequence'] < 1:
                            raise ValueError('Invalid acknowledgement')
                        with self.connect() as c:
                            c.execute('UPDATE outbox SET delivered_at=?,server_sequence=?,attempts=attempts+1,last_error=NULL WHERE event_id=?',
                                      (_now(), ack['sequence'], row['event_id']))
                    except Exception as exc:
                        status = exc.code if isinstance(exc, HTTPError) else None
                        blocked = status in (400, 409, 413, 415, 422)
                        # Do not persist provider bodies or raw exceptions.
                        if status == 403:
                            message = 'Central HTTP 403: this PC name is blocked in Central; kept until it is unblocked'
                        else:
                            message = 'Central HTTP ' + str(status) if status else 'Central unavailable or acknowledgement invalid; retained for retry'
                        with self.connect() as c:
                            c.execute('UPDATE outbox SET attempts=attempts+1,last_error=?,blocked=? WHERE event_id=?', (message, int(blocked), row['event_id']))
                        if not blocked: return
        finally:
            self.flush_lock.release()

    def say_hello(self):
        """Keeps this PC listed as connected in Central even when it has nothing to upload."""
        if time.monotonic() < self.next_hello: return
        self.next_hello = time.monotonic() + HELLO_SECONDS
        try: self.greeter(self.config, {'client_name': self.client_name, 'computer': socket.gethostname(), 'actor': getpass.getuser()})
        except Exception: pass

    def _run(self):
        while not self.stop_event.is_set():
            self.wake.clear()
            try: self.flush_once()
            except Exception: pass  # Preserve pending rows; no GUI calls from this thread.
            self.say_hello()
            self.wake.wait(self.interval)

    def close(self):
        self.stop_event.set(); self.wake.set()
        if self.thread: self.thread.join(timeout=12)


def configured_sync(base: Path, cached_orders: dict[str, dict] | None = None):
    import config
    if config.CENTRAL_URL.strip().lower() in ('', 'off'):
        return None
    ca_file = None
    if config.CENTRAL_CA_FILE:
        ca = Path(config.CENTRAL_CA_FILE)
        ca_file = str(ca if ca.is_absolute() else Path(base) / ca)
    sync = CentralSync(base, {'url': config.CENTRAL_URL, 'ca_file': ca_file, 'client_name': config.CENTRAL_CLIENT_NAME})
    try:
        sync.import_past_orders_once(cached_orders or {})
    except Exception:
        # Live uploads without the one-time import would be imported again as past orders next launch.
        sync.close()
        raise
    return sync


if __name__ == '__main__':
    base = Path(__file__).resolve().parent
    db = base / '.fedfish-central' / 'outbox.sqlite3'
    if not db.exists():
        print('No queue yet. Start FedFish; CENTRAL_URL=off in .env turns uploads off.')
    else:
        c = sqlite3.connect('file:' + db.as_posix() + '?mode=ro', uri=True)
        try:
            events, pending, rejected = c.execute(
                'SELECT COUNT(*), SUM(delivered_at IS NULL AND blocked=0), SUM(blocked) FROM outbox').fetchone()
            error = c.execute('SELECT last_error FROM outbox WHERE delivered_at IS NULL AND last_error IS NOT NULL '
                              'ORDER BY created_at DESC LIMIT 1').fetchone()
            try:
                imported = c.execute('SELECT value FROM meta WHERE key=?', (PAST_ORDERS_IMPORTED,)).fetchone()
            except sqlite3.OperationalError:
                imported = None
            print(json.dumps({'events': events, 'pending': pending or 0, 'rejected': rejected or 0,
                              'last_error': error[0] if error else None,
                              'past_orders_imported_at': imported[0] if imported else None}))
        finally:
            c.close()
