#!/usr/bin/env python3
"""Native process/API/restart/replica checks. Uses only Python standard library.
Pass a built university-demo binary. Temporary stores are created under --output.
"""
import argparse
import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True)
parser.add_argument('--output', default='artifacts/api-smoke')
args = parser.parse_args()
binary = str(Path(args.binary).resolve())
root = Path(args.output).resolve()
root.mkdir(parents=True, exist_ok=False)
checks = []
processes = []

def cli(*items, success=True):
    result = subprocess.run([binary, *map(str, items)], text=True, capture_output=True, timeout=60)
    assert (result.returncode == 0) == success, (items, result.stdout, result.stderr)
    return result

def start(store, seed=False):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    log = (root / f'server-{port}.log').open('w')
    command = [binary, 'serve', '--data', str(store), '--port', str(port)]
    if seed:
        command.append('--seed-demo')
    child = subprocess.Popen(command, stdout=log, stderr=log)
    processes.append((child, log))
    url = f'http://127.0.0.1:{port}'
    for _ in range(100):
        if child.poll() is not None:
            raise AssertionError(f'Server exited: {root / f"server-{port}.log"}')
        try:
            if call(url, '/api/health')[0] == 200:
                return child, url
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise AssertionError('Server did not become ready')

def stop(child):
    child.terminate()
    child.wait(timeout=10)

def call(base, path, method='GET', payload=None, headers=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(base + path, data=body, method=method,
        headers={'Content-Type': 'application/json', **(headers or {})})
    try:
        response = urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        value = json.loads(raw) if response.headers.get_content_type() == 'application/json' else raw.decode()
        return response.status, value

try:
    writer = root / 'writer'
    child, url = start(writer, seed=True)
    _, health = call(url, '/api/health')
    assert health['runtime'] == 'rust' and health['stats']['all'] == 6
    key = health['info']['public_key']
    assert len(key) == 64 and health['info']['writable']
    checks.append('Native seeded Rust server; six fictional projects')
    assert call(url, '/api/projects', headers={'Origin': 'https://untrusted.invalid'})[0] == 403
    assert call(url, '/api/projects', headers={'Host': 'untrusted.invalid'})[0] == 403
    assert call(url, '/api/projects', headers={'Origin': url})[0] == 200
    checks.append('Cross-origin and DNS-rebinding Host rejected; same-origin accepted')
    original_length = health['info']['length']
    new = {'id': 'api-test', 'title': 'API regression', 'summary': 'Fictional record',
           'course': 'QA-1', 'supervisor': 'Demo supervisor', 'team': ['Demo team'], 'actor': 'QA'}
    status, project = call(url, '/api/projects', 'POST', new)
    assert status == 201 and project['version'] == 1 and project['status'] == 'planned'
    assert call(url, '/api/projects', 'POST', new)[0] == 409
    assert call(url, '/api/projects/api-test', 'PATCH', {'actor': 'QA', 'expected_version': 0, 'title': 'stale'})[0] == 409
    assert call(url, '/api/projects/api-test', 'PATCH', {'actor': 'QA', 'expected_version': 1, 'status': 'completed'})[0] == 400
    assert call(url, '/api/health')[1]['info']['length'] == original_length + 1
    checks.append('Invalid transitions, duplicate IDs and stale edits do not append')
    status, project = call(url, '/api/projects/api-test', 'PATCH', {'actor': 'QA', 'expected_version': 1, 'status': 'active'})
    assert status == 200 and project['version'] == 2
    status, project = call(url, '/api/projects/api-test/archive', 'POST', {'actor': 'QA', 'expected_version': 2, 'reason': 'QA completed'})
    assert status == 200 and project['version'] == 3
    assert call(url, '/api/projects/api-test', 'PATCH', {'actor': 'QA', 'expected_version': 3, 'title': 'illegal'})[0] == 400
    assert len(call(url, '/api/projects/api-test/history')[1]) == 3
    assert len(call(url, '/api/projects?search=regression&status=archived&course=QA-1')[1]) == 1
    assert call(url, '/api/projects/missing/history')[0] == 404
    checks.append('Create/edit/archive/history/search use real append-only events')
    status, audit = call(url, '/api/audit', 'POST')
    assert status == 200 and audit['signed_head_verified'] and audit['missing_blocks'] == 0
    bundle = call(url, '/api/export')[1]
    assert bundle['length'] == audit['verified_blocks'] == original_length + 3
    assert all(name not in json.dumps(bundle) for name in ['secret_key', 'private_key', 'signing_key'])
    (root / 'public-bundle.json').write_text(json.dumps(bundle, indent=2))
    checks.append('Audit verifies all blocks; export contains public proofs')
    stop(child)
    cli('list', '--data', writer)
    child, url = start(writer)
    assert call(url, '/api/projects/api-test')[1] == project
    stop(child)
    checks.append('Process restart rebuilds identical archived project')
    wrong = root / 'wrong-replica'
    cli('replicate', '--source', writer, '--destination', wrong, '--writer-key', '00' * 32, success=False)
    assert not wrong.exists()
    replica = root / 'replica'
    cli('replicate', '--source', writer, '--destination', replica, '--writer-key', key)
    cli('verify', '--data', replica)
    child, url = start(replica)
    assert call(url, '/api/health')[1]['info']['writable'] is False
    assert call(url, '/api/projects/api-test')[1] == project
    assert call(url, '/api/projects', 'POST', {**new, 'id': 'cannot-write'})[0] == 403
    assert call(url, '/api/health')[1]['info']['length'] == bundle['length']
    stop(child)
    checks.append('Pinned-key CLI replication; actual reopened replica rejects HTTP writes')
    exported = root / 'cli-bundle.json'
    cli('export', '--data', writer, '--output', exported)
    before = exported.read_bytes()
    cli('export', '--data', writer, '--output', exported, success=False)
    assert exported.read_bytes() == before
    checks.append('CLI export refuses overwriting an existing file')
finally:
    for child, log in processes:
        if child.poll() is None:
            stop(child)
        log.close()
report = {'checks_passed': len(checks), 'checks': checks}
(root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
