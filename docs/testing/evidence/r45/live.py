import pathlib,subprocess,time,json,os,hashlib
p=pathlib.Path('/var/tmp/r45');results=[];children=[]
def check(name,command,expected=0,contains=None,env=None):
 r=subprocess.run(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=35)
 ok=r.returncode==expected and (contains is None or contains in r.stdout)
 results.append({'check':name,'exit':r.returncode,'expectedExit':expected,'pass':ok,'output':r.stdout})
 (p/'live-evidence.json').write_text(json.dumps({'case':'2.1-29','checks':results},indent=2));print(name, 'PASS' if ok else 'FAIL',r.stdout[-500:],flush=True);assert ok,name
unit='r45-chrony.service'
def start_daemon(sources):
 (p/'chrony').mkdir(exist_ok=True);(p/'chrony').chmod(0o700);(p/'chrony'/'conf').write_text('port 0\ncmdport 0\npidfile /run/chrony/chronyd.pid\nbindcmdaddress /run/chrony/chronyd.sock\n'+sources)
 subprocess.run(['systemctl','stop',unit],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 subprocess.run(['systemd-run','--unit=r45-chrony','--collect','-p','BindReadOnlyPaths=/var/tmp/r45/chrony/conf:/etc/chrony/chrony.conf','-p','BindPaths=/var/tmp/r45/chrony:/run/chrony','/usr/sbin/chronyd','-n','-x','-u','root','-f','/etc/chrony/chrony.conf'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 for _ in range(30):
  if (p/'chrony'/'chronyd.sock').exists():break
  time.sleep(.1)
 time.sleep(.3)
def test(binary,want='',namespace=None):
 env=os.environ.copy();env['KC_READINESS_LIVE']='1';env['KC_READINESS_WANT']=want
 cmd=[str(p/binary),'-test.v','-test.run=^TestNodeReadinessStep$' if binary=='k8s.test' else '-test.run=^TestLiveNTPPrecheck$']
 if namespace:cmd=['unshare','--mount','--uts','--propagation','private','bash','-c',namespace+'\nexec "$@"','r45']+cmd
 return cmd,env
try:
 for binary in ['deploy.test','k8s.test']:
  cmd,env=test(binary);check('healthy '+binary,cmd,env=env)
 cmd,env=test('k8s.test','insufficient disk space', 'mount -t tmpfs -o size=64m r45-low-space /usr/local')
 check('real low-space filesystem rejected before writes',cmd,contains='insufficient disk space',env=env)
 # custom etcd path demonstrates checking each selected filesystem, not just root.
 (p/'etcd custom').mkdir(exist_ok=True)
 cmd,env=test('k8s.test','insufficient disk space','mount -t tmpfs -o size=64m r45-low-space "/var/tmp/r45/etcd custom"')
 check('custom etcd filesystem low space rejected',cmd,contains='etcd custom',env=env)
 cmd,env=test('k8s.test','invalid Kubernetes node hostname',"python3 -c 'import ctypes; assert ctypes.CDLL(None).sethostname(b\"r45_bad_hostname\",16)==0'")
 check('real invalid UTS hostname rejected',cmd,contains='invalid Kubernetes node hostname',env=env)
 start_daemon('server 127.0.0.1 port 18124 iburst\n')
 check('isolated unsynchronized chronyd is active',['systemctl','is-active',unit],contains='active')
 namespace='mount --bind /var/tmp/r45/chrony /run/chrony'
 check('old service-active check incorrectly accepts unsynchronized daemon',['unshare','--mount','--propagation','private','bash','-c',namespace+'\nfor service in chrony chronyd ntp ntpd systemd-timesyncd; do systemctl is-active --quiet "$service" && exit 0; done; exit 10'])
 for binary in ['deploy.test','k8s.test']:
  cmd,env=test(binary,'synchronization is not confirmed',namespace);check('unsynchronized daemon rejected '+binary,cmd,contains='synchronization is not confirmed',env=env)
 server=subprocess.Popen(['python3',str(p/'ntp-fault.py')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);children.append(server);time.sleep(.3)
 start_daemon('server 127.0.0.1 port 18123 iburst minpoll 0 maxpoll 0\n')
 for _ in range(40):
  r=subprocess.run(['unshare','--mount','--propagation','private','bash','-c',namespace+'\nchronyc tracking'],capture_output=True,text=True)
  if 'Normal' in r.stdout:break
  time.sleep(.3)
 check('real chronyd selected faulty NTP source',['unshare','--mount','--propagation','private','bash','-c',namespace+'\nchronyc tracking'],contains='Normal')
 for binary in ['deploy.test','k8s.test']:
  cmd,env=test(binary,'synchronization is not confirmed',namespace);check('60-second source correction rejected '+binary,cmd,contains='synchronization is not confirmed',env=env)
 print('ALL LIVE CHECKS PASSED',flush=True)
finally:
 subprocess.run(['systemctl','stop',unit],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 for child in children:child.terminate();child.wait(timeout=5)
 check('original chrony synchronization remains healthy',['chronyc','waitsync','1','5','0','.1'])
 check('original agent active',['systemctl','is-active','kc-agent'],contains='active')
 check('no leaked low-space mount',['bash','-c','! findmnt -rn -t tmpfs | grep r45-low-space'])
