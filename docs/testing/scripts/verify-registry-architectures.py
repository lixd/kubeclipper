import argparse
import hashlib
import io
import json
from pathlib import Path
import struct
import tarfile

parser = argparse.ArgumentParser(description='Verify amd64 and arm64 Registry binaries in the bootstrap archive.')
parser.add_argument('--bundle', required=True, help='Registry bootstrap archive')
parser.add_argument('--output', required=True, help='JSON evidence output file')
args = parser.parse_args()
with tarfile.open(args.bundle, 'r:gz') as archive:
    files = {member.name: archive.extractfile(member).read()
             for member in archive if member.isfile()}


def blob(digest):
    data = files['blobs/' + digest.replace(':', '/')]
    assert 'sha256:' + hashlib.sha256(data).hexdigest() == digest
    return data


index = json.loads(files['index.json'])
assert len(index['manifests']) == 1
platforms = json.loads(blob(index['manifests'][0]['digest']))['manifests']
records = []
for arch, expected in [('amd64', 62), ('arm64', 183)]:
    selected = [p for p in platforms if p['platform']['os'] == 'linux'
                and p['platform']['architecture'] == arch]
    assert len(selected) == 1
    image = json.loads(blob(selected[0]['digest']))
    config = json.loads(blob(image['config']['digest']))
    # Data package images identify the payload platform in the index and
    # kc-package-manifest; the current publisher leaves config platform empty.
    assert config.get('os') in ('', 'linux')
    assert config.get('architecture') in ('', arch)
    contents = {}
    for layer in image['layers']:
        with tarfile.open(fileobj=io.BytesIO(blob(layer['digest'])), mode='r:*') as payload:
            for member in payload:
                name = member.name.lstrip('./')
                if name in ('opt/kubeclipper/resource/registry',
                            'opt/kubeclipper/resource/kc-package-manifest.json'):
                    contents[name] = payload.extractfile(member).read()
    binary = contents['opt/kubeclipper/resource/registry']
    metadata = json.loads(contents['opt/kubeclipper/resource/kc-package-manifest.json'])
    assert metadata['kind'] == 'bootstrap' and metadata['name'] == 'registry'
    assert metadata['platform']['os'] == 'linux' and metadata['platform']['arch'] == arch
    assert binary[:4] == b'\x7fELF'
    actual = struct.unpack('<H', binary[18:20])[0]
    assert actual == expected, (arch, actual)
    digest = 'sha256:' + hashlib.sha256(binary).hexdigest()
    assert any(c['name'] == 'registry' and c['digest'] == digest for c in metadata['contents'])
    records.append({'platform': 'linux/' + arch, 'elfMachine': actual,
                    'imageConfigOS': config.get('os'), 'imageConfigArch': config.get('architecture'),
                    'binaryDigest': digest, 'packageManifestDigestMatches': True})
Path(args.output).write_text(json.dumps(records, indent=2))
print(json.dumps(records, indent=2))
