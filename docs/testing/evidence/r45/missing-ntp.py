import subprocess,os,json,pathlib
p=pathlib.Path('/var/tmp/r45');checks=[]
try:
 subprocess.run(['systemctl','stop','chrony'],check=True)
 for binary,test in [('deploy.test','TestLiveNTPPrecheck'),('k8s.test','TestNodeReadinessStep')]:
  env=os.environ.copy();env.update(KC_READINESS_LIVE='1',KC_READINESS_WANT='no supported time synchronization service')
  r=subprocess.run([str(p/binary),'-test.v','-test.run=^'+test+'$'],capture_output=True,text=True,env=env,timeout=20)
  ok=r.returncode==0;checks.append({'check':'NTP service missing '+binary,'exit':r.returncode,'pass':ok,'output':r.stdout+r.stderr});print(binary,'PASS' if ok else 'FAIL',flush=True);assert ok
finally:
 subprocess.run(['systemctl','start','chrony'],check=True)
 r=subprocess.run(['chronyc','waitsync','60','5','0','.5'],capture_output=True,text=True,timeout=35);checks.append({'check':'original chrony restored and synchronized','exit':r.returncode,'pass':r.returncode==0,'output':r.stdout});(p/'missing-ntp-evidence.json').write_text(json.dumps({'checks':checks},indent=2));assert r.returncode==0
