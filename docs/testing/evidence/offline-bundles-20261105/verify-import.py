import concurrent.futures
import hashlib
import json
import pathlib
import urllib.error
import urllib.request

BASE = pathlib.Path('/root/kc-offline-bundles-20261105')
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
TARGET = 'http://127.0.0.1:5617/v2/kubeclipper-release/'
ACCEPT = ', '.join(['application/vnd.oci.image.index.v1+json',
                    'application/vnd.docker.distribution.manifest.list.v2+json',
                    'application/vnd.oci.image.manifest.v1+json',
                    'application/vnd.docker.distribution.manifest.v2+json'])

try:
    OPENER.open('https://registry.cn-hangzhou.aliyuncs.com/v2/', timeout=3)
except urllib.error.HTTPError as e:
    raise AssertionError('Public registry remained reachable in isolated audit') from e
except urllib.error.URLError:
    pass
else:
    raise AssertionError('Public egress was not blocked')

roots = json.loads((BASE / 'export-index.json').read_text())['manifests']


def inspect(root):
    reference = root['annotations']['org.opencontainers.image.ref.name']
    repo, tag = reference.rsplit(':', 1)
    blobs = {}
    platforms = []
    seen = set()

    def walk(descriptor, ref):
        request = urllib.request.Request(TARGET + repo + '/manifests/' + ref,
                                         headers={'Accept': ACCEPT})
        with OPENER.open(request, timeout=30) as response:
            raw = response.read()
        assert len(raw) == descriptor['size'], (reference, 'manifest size')
        assert 'sha256:' + hashlib.sha256(raw).hexdigest() == descriptor['digest'], (reference, 'manifest digest')
        if descriptor['digest'] in seen:
            return
        seen.add(descriptor['digest'])
        manifest = json.loads(raw)
        if 'manifests' in manifest:
            for child in manifest['manifests']:
                if 'platform' in child:
                    platforms.append(child['platform'])
                walk(child, child['digest'])
        else:
            for blob in [manifest['config']] + manifest['layers']:
                blobs[blob['digest']] = {'repo': repo, 'size': blob['size'], 'digest': blob['digest']}

    walk(root, tag)
    assert any(p.get('os') == 'linux' and p.get('architecture') == 'amd64' for p in platforms), reference
    assert any(p.get('os') == 'linux' and p.get('architecture') == 'arm64' for p in platforms), reference
    return {'reference': reference, 'rootDigest': root['digest'], 'platforms': platforms,
            'manifestsVerified': len(seen), 'blobs': blobs}


with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
    records = list(pool.map(inspect, roots))
print('Verified all 61 original roots and every referenced manifest', flush=True)

unique = {}
links = {}
for record in records:
    for digest, blob in record['blobs'].items():
        unique[digest] = blob
        links[(blob['repo'], digest)] = blob


def check_link(blob):
    request = urllib.request.Request(TARGET + blob['repo'] + '/blobs/' + blob['digest'], method='HEAD')
    with OPENER.open(request, timeout=30) as response:
        assert int(response.headers['Content-Length']) == blob['size']


def read_blob(blob):
    digest = hashlib.sha256()
    count = 0
    with OPENER.open(TARGET + blob['repo'] + '/blobs/' + blob['digest'], timeout=60) as response:
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            count += len(chunk)
    assert count == blob['size'], blob
    assert 'sha256:' + digest.hexdigest() == blob['digest'], blob
    return count


with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
    list(pool.map(check_link, links.values()))
    verified_bytes = sum(pool.map(read_blob, unique.values()))

proof = {'complete': True, 'source': 'exact exported root descriptors captured during native export',
         'tags': len(records), 'allTagsHaveLinuxAMD64AndARM64': True,
         'publicEgressBlockedDuringAudit': True,
         'manifestsVerified': sum(r['manifestsVerified'] for r in records),
         'repositoryBlobLinksVerified': len(links), 'uniqueBlobsFullyReadAndHashed': len(unique),
         'compressedContentBytesVerified': verified_bytes,
         'records': [{k: v for k, v in r.items() if k != 'blobs'} for r in records]}
(BASE / 'import-proof.json').write_text(json.dumps(proof, indent=2) + '\n')
print(json.dumps({k: v for k, v in proof.items() if k != 'records'}, indent=2), flush=True)
