#!/usr/bin/env python3
"""Start disposable loopback Velocity/BSync, restart BSync, verify channel delivery recovers."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
import yaml

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
JAVA = Path(os.environ['JAVA_HOME']) / 'bin'


def port():
    with socket.socket() as value:
        value.bind(('127.0.0.1', 0))
        return value.getsockname()[1]


def wait(predicate, process=None, seconds=40):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        if process is not None and process.poll() is not None:
            raise RuntimeError('Test process exited early')
        time.sleep(.1)
    raise TimeoutError('Test condition timed out')


def stop(process, command):
    if process is None or process.poll() is not None:
        return
    try:
        process.stdin.write(command + '\n')
        process.stdin.flush()
        process.wait(timeout=15)
    except (BrokenPipeError, subprocess.TimeoutExpired):
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    proxy_jar = Path(sys.argv[1]).resolve()
    work = Path(tempfile.mkdtemp(prefix='bsync-velocity-'))
    proxy = work / 'proxy'
    backend = work / 'backend'
    (proxy / 'plugins/bsync').mkdir(parents=True)
    backend.mkdir()
    backend_port, proxy_port = port(), port()
    (backend / 'config.yml').write_text(yaml.safe_dump({'port': backend_port, 'database': {
        'type': 'h2', 'user': 'sa', 'password': '', 'host': 'localhost', 'port': 3306, 'database': 'test'}}))
    (proxy / 'velocity.toml').write_text(f'''config-version = "2.9"
bind = "127.0.0.1:{proxy_port}"
online-mode = false
player-info-forwarding-mode = "NONE"
forwarding-secret-file = "forwarding.secret"
[servers]
lobby = "127.0.0.1:1"
factions = "127.0.0.1:1"
minigames = "127.0.0.1:1"
try = ["lobby"]
''')
    module_jar = ROOT / 'target/bsync-velocity-1.6.jar'
    shutil.copy2(module_jar, proxy / 'plugins/bsync.jar')
    cp = os.pathsep.join([str(module_jar), (ROOT / 'target/integration-classpath.txt').read_text().strip()])
    classes = work / 'classes'
    classes.mkdir()
    subprocess.run([str(JAVA / 'javac'), '-proc:none', '--release', '21', '-cp', cp, '-d', str(classes),
                    *map(str, (ROOT / 'integration/src').rglob('*.java'))], check=True)
    with zipfile.ZipFile(proxy / 'plugins/probe.jar', 'w') as out:
        for file in classes.rglob('*.class'):
            out.write(file, file.relative_to(classes))
        out.writestr('velocity-plugin.json', json.dumps({'id': 'bsync-probe', 'name': 'BSyncProbe', 'version': '1',
            'main': 'dev.by1337.sync.velocity.integration.Probe', 'dependencies': [{'id': 'bsync', 'optional': False}]}))
    logs = []
    processes = []
    def launch_backend():
        log = (backend / ('console-' + str(len(logs)) + '.log')).open('w')
        logs.append(log)
        process = subprocess.Popen([str(JAVA / 'java'), '-Xmx256m', '-jar',
            str(REPO / 'bsync-server/target/bsync-server-1.6.jar')], cwd=backend,
            stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT, text=True)
        processes.append(process)
        wait(lambda: (backend / 'keys.yml').exists(), process)
        return process
    backend_process = proxy_process = None
    try:
        # Generate an isolated keypair, then prove the proxy starts with its backend unavailable.
        backend_process = launch_backend()
        key = yaml.safe_load((backend / 'keys.yml').read_text())['private_key']
        stop(backend_process, 'stop')
        (proxy / 'plugins/bsync/config.yml').write_text(f'''id: '{{SERVER_ID}}'
workers: 1
netty_threads: 1
servers:
  main:
    group: network
    ip: 127.0.0.1
    port: {backend_port}
    private_key: '{{BSYNC_PRIVATE_KEY}}'
''')
        env = dict(os.environ, SERVER_ID='velocity-env-probe', BSYNC_PRIVATE_KEY=key)
        log = (proxy / 'console.log').open('w')
        logs.append(log)
        proxy_process = subprocess.Popen([str(JAVA / 'java'), '-Xmx512m', '-jar', str(proxy_jar)], cwd=proxy,
            env=env, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT, text=True)
        processes.append(proxy_process)
        markers = proxy / 'plugins/bsync-probe'
        wait(lambda: (markers / 'initialized').exists(), proxy_process)
        assert proxy_process.poll() is None
        backend_process = launch_backend()
        wait(lambda: (markers / 'round-1').exists(), proxy_process)
        stop(backend_process, 'stop')
        wait(lambda: (markers / 'inactive').exists(), proxy_process)
        assert proxy_process.poll() is None
        backend_process = launch_backend()
        wait(lambda: (markers / 'round-2').exists(), proxy_process)
        print('PASS: offline startup, env, authenticated delivery, backend restart and channel recovery:', work)
        (ROOT / 'target/integration-result.txt').write_text('PASS\n' + str(work) + '\n')
    finally:
        stop(proxy_process, 'shutdown')
        stop(backend_process, 'stop')
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        print('Test files:', work)


if __name__ == '__main__':
    main()
