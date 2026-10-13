import ssl,json,urllib.request,urllib.error,urllib.parse,subprocess,time,re,pathlib,yaml,hashlib
p=pathlib.Path('/var/tmp/r44');ctx=ssl.create_default_context(cafile='/etc/kubeclipper-server/pki/ca.crt');base='https://127.0.0.1:18082';pw=json.loads((p/'credentials.json').read_text())['password'];checks=[]
def req(path,data=None,bearer=None,form=False,method=None):
 h={};raw=None
 if data is not None:
  h['Content-Type']='application/x-www-form-urlencoded' if form else 'application/json';raw=(urllib.parse.urlencode(data) if form else json.dumps(data)).encode()
 if bearer:h['Authorization']='Bearer '+bearer
 r=urllib.request.Request(base+path,data=raw,headers=h,method=method)
 try:
  with urllib.request.urlopen(r,context=ctx,timeout=15) as res:return res.status,json.loads(res.read() or b'{}')
 except urllib.error.HTTPError as e:
  raw=e.read()
  try:b=json.loads(raw)
  except:b={}
  return e.code,b

def check(name,status,expected,**more):
 checks.append({'check':name,'actual':status,'expected':expected,'pass':status==expected,**more});print(name,status,'PASS' if status==expected else 'FAIL',flush=True);save();assert status==expected,(name,status)
def save():
 (p/'evidence.json').write_text(json.dumps({'case':'4-08c','sourceRevision':'bf0decc5 (code unchanged from b4a5886e)','serverSHA256':hashlib.sha256((p/'kubeclipper-server').read_bytes()).hexdigest(),'tls':'CA and hostname verified','etcdPrefix':'/registry/r44-mfa','checks':checks},indent=2))
def login(password=pw,user='admin'):return req('/oauth/login',{'username':user,'password':password})
for _ in range(30):
 try:
  s,b=login()
  if s==200:break
 except Exception:pass
 time.sleep(1)
check('password login with MFA disabled',s,200);admin=b['access_token']
s,b=login('WrongR44!a');check('incorrect password rejected',s,401)
user={'apiVersion':'iam.kubeclipper.io/v1','kind':'User','metadata':{'name':'r44-mfa','annotations':{'iam.kubeclipper.io/role':'platform-view'}},'spec':{'email':'r44@example.invalid','phone':'13800004444','password':pw}}
req('/api/iam.kubeclipper.io/v1/users/r44-mfa',bearer=admin,method='DELETE')
s,b=req('/api/iam.kubeclipper.io/v1/users',user,admin);check('create isolated MFA test user',s,200)
c=yaml.safe_load((p/'config.yaml').read_text());c['authentication']['mfaOptions']['enabled']=True;(p/'config.yaml').write_text(yaml.safe_dump(c));subprocess.run(['systemctl','restart','r44-mfa'],check=True)
for _ in range(30):
 try:
  s,b=login(user='r44-mfa')
  if s==428:break
 except Exception:pass
 time.sleep(1)
check('password accepted and MFA challenge required',s,428);provider=b['providers'][0];assert provider['type']=='fake_sms'
def send():return req('/oauth/verification-code',provider)
def verify(code):return req('/oauth/token',{'grant_type':'mfa','mfa_provider':'fake_sms','token':provider['token'],'code':code},form=True)
def code():
 out=subprocess.check_output(['journalctl','-u','r44-mfa','--no-pager','-o','cat']).decode();return re.findall(r'fake SMS Verification code:(\d+) phone: 13800004444',out)[-1]
s,b=send();check('request verification code',s,200);correct=code();sent=time.monotonic()
s,b=send();check('immediate resend rate limited',s,429)
s,b=verify('000000' if correct!='000000' else '999999');check('incorrect verification code rejected',s,401)
s,b=verify(correct);check('correct verification code issues access token',s,200);assert b.get('access_token');token=b['access_token']
s,b=req('/api/iam.kubeclipper.io/v1/users/r44-mfa',bearer=token);check('issued access token authenticates real API',s,200)
s,b=verify(correct);check('used challenge/code cannot be replayed',s,401)
s,b=login(user='r44-mfa');check('fresh challenge for consumed-code check',s,428);provider=b['providers'][0]
s,b=verify(correct);check('consumed code rejected with fresh valid challenge',s,401)
print('waiting actual 60-second resend window',flush=True)
time.sleep(max(0,65-(time.monotonic()-sent)))
s,b=login(user='r44-mfa');check('new MFA challenge',s,428);provider=b['providers'][0]
s,b=send();check('resend succeeds after rate-limit window',s,200);expired=code();sent=time.monotonic()
print('waiting actual 60-second code expiry',flush=True)
time.sleep(max(0,65-(time.monotonic()-sent)))
s,b=verify(expired);check('expired code rejected after 65 seconds',s,401,waitSeconds=65)
s,b=send();check('new code request after expiry',s,200);fresh=code()
s,b=verify(fresh);check('fresh code logs in after expired-code rejection',s,200)
print('ALL CHECKS PASSED',flush=True)
