#!/usr/bin/env python3
"""Offline installer regressions; HTTP fixtures are not release validation."""
import hashlib
import http.server
import os
from pathlib import Path
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
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.existing = self.bin / 'kcctl'
        self.existing.write_bytes(b'existing-install')
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith('KC_') and k != 'ARCH'}
        self.env.update(KC_BIN_DIR=str(self.bin), KC_VERSION='v9.8.7', ARCH='amd64')

    def run_installer(self):
        return subprocess.run(['bash', str(SCRIPT)], env=self.env,
                              capture_output=True, text=True)

    def serve(self, bad_checksum=False, missing_binary=False):
        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(handler):
                requests.append(handler.path)
                if handler.path.endswith('/kcctl-checksums.txt'):
                    digest = '0' * 64 if bad_checksum else DIGEST
                    body = ''.join(f'{digest}  kcctl-{system}-amd64\n'
                                   for system in ('linux', 'darwin')).encode()
                elif not missing_binary:
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

    def test_checksum_failure_preserves_existing_binary(self):
        self.serve(bad_checksum=True)
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('SHA256 mismatch', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_http_failure_preserves_existing_binary(self):
        self.serve(missing_binary=True)
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Download failed', result.stderr)
        self.assertEqual(self.existing.read_bytes(), b'existing-install')

    def test_source_selection(self):
        # A curl stub records selected URLs and supplies explicit test fixtures.
        stub = self.root / 'tools'
        stub.mkdir()
        fixture = self.root / 'payload'
        fixture.write_bytes(PAYLOAD)
        checksums = self.root / 'checksums'
        checksums.write_text(''.join(f'{DIGEST}  kcctl-{s}-{a}\n'
                                    for s in ('linux', 'darwin')
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
        for region, override, expected in (
            ('', '', 'https://github.com/kubeclipper/kubeclipper/releases/download'),
            ('cn', '', 'https://release.kubeclipper.io/kc'),
            ('cn', 'https://mirror.example/kc/', 'https://mirror.example/kc'),
        ):
            with self.subTest(region=region, override=override):
                self.env.update(KC_REGION=region, KC_DOWNLOAD_URL=override, ARCH='aarch64')
                log.write_text('')
                result = self.run_installer()
                self.assertEqual(result.returncode, 0, result.stderr)
                urls = log.read_text().splitlines()
                self.assertEqual(len(urls), 2)
                self.assertTrue(all(u.startswith(expected + '/v9.8.7/') for u in urls))
                self.assertTrue(urls[0].endswith('-arm64'))


if __name__ == '__main__':
    unittest.main()
