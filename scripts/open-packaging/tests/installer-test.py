#!/usr/bin/env python3
"""Offline installer regressions; HTTP fixtures are not release validation."""
import hashlib
import http.server
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / 'scripts/get-kubeclipper.sh'
PAYLOAD = b'#!/bin/sh\necho fixture-kcctl\n'
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


class InstallerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.script = SCRIPT
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.existing = self.bin / 'kcctl'
        self.existing.write_bytes(b'existing-install')
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith('KC_') and k not in ('ARCH', 'KCZONE')}
        self.env.update(KC_BIN_DIR=str(self.bin), KC_VERSION='v9.8.7', ARCH='amd64')
        tools = self.root / 'tools'
        tools.mkdir()
        uname = tools / 'uname'
        uname.write_text('''#!/usr/bin/env bash
case "$1" in
  -s) printf '%s\\n' "$TEST_OS" ;;
  -m) printf '%s\\n' x86_64 ;;
esac
''')
        uname.chmod(0o755)
        self.env.update(PATH=f'{tools}:{self.env["PATH"]}', TEST_OS='Linux')

    def run_installer(self, *args):
        return subprocess.run(['bash', str(self.script), *args], env=self.env,
                              capture_output=True, text=True)

    def serve(self, bad_checksum=False, missing_binary=False, transient_failure=False, versions=('v9.8.7',)):
        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(handler):
                requests.append(handler.path)
                if transient_failure and len(requests) == 1:
                    handler.send_error(503)
                    return
                if handler.path in {f'/{version}/kcctl-checksums.txt' for version in versions}:
                    digest = '0' * 64 if bad_checksum else DIGEST
                    body = f'{digest}  kcctl-linux-amd64\n'.encode()
                elif handler.path in {f'/{version}/kcctl-linux-amd64' for version in versions} and not missing_binary:
                    body = PAYLOAD
                else:
                    handler.send_error(404)
                    return
                handler.send_response(200)
                handler.end_headers()
                handler.wfile.write(body)

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.env['KC_DOWNLOAD_URL'] = f'http://127.0.0.1:{server.server_port}'
        return requests

    def test_install_verifies_and_replaces_binary(self):
        requests = self.serve()
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.existing.read_bytes(), PAYLOAD)
        self.assertTrue(os.access(self.existing, os.X_OK))
        self.assertEqual(len(requests), 2)
        self.assertTrue(all(p.startswith('/v9.8.7/') for p in requests))
        self.assertEqual(requests[0], '/v9.8.7/kcctl-linux-amd64')

    def test_default_version_and_explicit_override(self):
        self.script = self.root / 'get-kubeclipper.sh'
        self.script.write_text(re.sub(r'^DEFAULT_VERSION=.*$', 'DEFAULT_VERSION="v8.7.6"',
                                     SCRIPT.read_text(), count=1, flags=re.MULTILINE))
        requests = self.serve(versions=('v8.7.6', 'v9.8.7'))
        self.env.pop('KC_VERSION')
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(requests, ['/v8.7.6/kcctl-linux-amd64', '/v8.7.6/kcctl-checksums.txt'])
        requests.clear()
        self.env['KC_VERSION'] = 'v9.8.7'
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(requests, ['/v9.8.7/kcctl-linux-amd64', '/v9.8.7/kcctl-checksums.txt'])

    def test_checksum_failure_preserves_existing_binary(self):
        self.serve(bad_checksum=True)
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('SHA256 mismatch', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_transient_http_failure_is_retried(self):
        requests = self.serve(transient_failure=True)
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.existing.read_bytes(), PAYLOAD)
        self.assertEqual(requests[0], requests[1])
        self.assertEqual(len(requests), 3)

    def test_http_failure_preserves_existing_binary(self):
        self.serve(missing_binary=True)
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Download failed', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_source_selection(self):
        # A curl stub records selected URLs and supplies explicit test fixtures.
        stub = self.root / 'tools'
        fixture = self.root / 'payload'
        fixture.write_bytes(PAYLOAD)
        checksums = self.root / 'checksums'
        checksums.write_text(''.join(f'{DIGEST}  kcctl-linux-{a}\n'
                                    for a in ('amd64', 'arm64')))
        log = self.root / 'urls'
        curl = stub / 'curl'
        curl.write_text('''#!/usr/bin/env bash
set -eu
while [[ "$1" != -o ]]; do shift; done
output=$2
url=$3
printf '%s\\n' "$url" >> "$TEST_URL_LOG"
case "$url" in
  */kcctl-checksums.txt) cp "$TEST_CHECKSUMS" "$output" ;;
  *) cp "$TEST_PAYLOAD" "$output" ;;
esac
''')
        curl.chmod(0o755)
        self.env.update(PATH=f'{stub}:{self.env["PATH"]}', TEST_URL_LOG=str(log),
                        TEST_CHECKSUMS=str(checksums), TEST_PAYLOAD=str(fixture))
        for settings, expected in (
            ({}, 'https://github.com/kubeclipper/kubeclipper/releases/download'),
            ({'KCZONE': 'global'}, 'https://github.com/kubeclipper/kubeclipper/releases/download'),
            ({'KCZONE': 'cn'}, 'https://kubeclipper.oss-ap-southeast-1.aliyuncs.com/kc'),
            ({'KC_DOWNLOAD_URL': 'https://download.example.com/kc/'}, 'https://download.example.com/kc'),
            ({'KCZONE': 'cn', 'KC_DOWNLOAD_URL': 'https://download.example.com/kc'}, 'https://download.example.com/kc'),
            ({'KCZONE': 'cn', 'KC_DOWNLOAD_URL': 'https://github.com/fixture/repo/releases/download/'},
             'https://github.com/fixture/repo/releases/download'),
        ):
            for machine, arch in (('amd64', 'amd64'), ('x86_64', 'amd64'),
                                  ('arm64', 'arm64'), ('aarch64', 'arm64')):
                with self.subTest(settings=settings, machine=machine):
                    self.env.pop('KCZONE', None)
                    self.env.pop('KC_DOWNLOAD_URL', None)
                    self.env.update(settings, ARCH=machine)
                    log.write_text('')
                    result = self.run_installer()
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(log.read_text().splitlines(), [
                        f'{expected}/v9.8.7/kcctl-linux-{arch}',
                        f'{expected}/v9.8.7/kcctl-checksums.txt',
                    ])

    def test_unsupported_os_preserves_existing_binary(self):
        for system in ('Darwin', 'FreeBSD'):
            with self.subTest(system=system):
                self.env['TEST_OS'] = system
                result = self.run_installer()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Only Linux is supported', result.stderr)
                self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_unsupported_arch_preserves_existing_binary(self):
        self.env['ARCH'] = 'riscv64'
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Only amd64 and arm64 are supported', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_invalid_zone_is_rejected_without_replacing_binary(self):
        self.env['KCZONE'] = 'unknown'
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('KCZONE must be global or cn', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_missing_domestic_source_requires_explicit_download_url(self):
        self.script = self.root / 'get-kubeclipper.sh'
        self.script.write_text(SCRIPT.read_text().replace(
            'DEFAULT_CN_DOWNLOAD_URL="https://kubeclipper.oss-ap-southeast-1.aliyuncs.com/kc"',
            'DEFAULT_CN_DOWNLOAD_URL=""'))
        self.env['KCZONE'] = 'cn'
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Download source is not configured', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')
        self.serve()
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.existing.read_bytes(), PAYLOAD)

    def test_invalid_download_url_is_rejected_without_replacing_binary(self):
        self.env['KC_DOWNLOAD_URL'] = 'file:///tmp/kcctl'
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('HTTP or HTTPS release base URL', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')



if __name__ == '__main__':
    unittest.main()
