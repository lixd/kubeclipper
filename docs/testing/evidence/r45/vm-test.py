import pathlib,subprocess,time,json,os
p=pathlib.Path('/var/tmp/r45');checks=[]
ssh=['ssh','-o','StrictHostKeyChecking=no','-o','UserKnownHostsFile=/dev/null','-o','ConnectTimeout=5','-i',str(p/'vm-key'),'-p','22745','root@127.0.0.1']
def check(name,cmd,env=None,contains=None):
 r=subprocess.run(cmd,capture_output=True,text=True,env=env,timeout=20);out=r.stdout+r.stderr;ok=r.returncode==0 and (contains is None or contains in out);checks.append({'check':name,'exit':r.returncode,'pass':ok,'output':out});(p/'vm-evidence.json').write_text(json.dumps({'case':'2.1-29','isolation':'KVM guest CLOCK_REALTIME, not host clock','checks':checks},indent=2));print(name,'PASS' if ok else 'FAIL',out[-700:],flush=True);assert ok
for _ in range(20):
 r=subprocess.run(ssh+['date +%s'],capture_output=True,timeout=8)
 if r.returncode==0:break
 time.sleep(.5)
env=os.environ.copy();env.update(KC_TIME_LAG_LIVE='1',KC_TIME_LAG_HOST='127.0.0.1:22745',KC_TIME_LAG_KEY=str(p/'vm-key'))
cmd=[str(p/'deploy.test'),'-test.v','-test.run=^TestLiveTimeLagPrecheck$']
check('healthy real KVM guest clock accepted',cmd,env)
for offset in [60,-60]:
 check('set guest clock offset '+str(offset),ssh+['date -s @'+str(int(time.time())+offset)])
 env['KC_TIME_LAG_WANT']='exceeds 5s';check('real guest clock offset '+str(offset)+' rejected',cmd,env,contains='exceeds 5s')
check('restore guest clock',ssh+['date -s @'+str(int(time.time()))]);env['KC_TIME_LAG_WANT']='';check('restored guest clock accepted',cmd,env)
check('host clock remains synchronized',['chronyc','waitsync','1','5','0','.1'])
print('ALL KVM CLOCK CHECKS PASSED')
