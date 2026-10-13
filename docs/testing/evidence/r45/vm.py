import pathlib,subprocess,shutil,re,os
p=pathlib.Path('/var/tmp/r45');r=p/'initramfs'
if r.exists():shutil.rmtree(r)
r.mkdir()
for d in ['bin','sbin','etc','proc','sys','dev','tmp','run','root/.ssh','etc/dropbear']:(r/d).mkdir(parents=True,exist_ok=True)
shutil.copy('/usr/bin/busybox',r/'bin/busybox')
for app in ['sh','mount','date','hostname','sleep','ip','ifconfig','udhcpc','cat','mkdir','chmod','grep','cut','awk','poweroff','uname','setsid','echo']:
 (r/'bin'/app).symlink_to('busybox')
shutil.copy('/usr/sbin/dropbear',r/'sbin/dropbear')
libs=re.findall(r'(/[\w/.-]+)',subprocess.check_output(['ldd','/usr/sbin/dropbear']).decode())
for lib in set(libs):
 path=r/lib.lstrip('/');path.parent.mkdir(parents=True,exist_ok=True);shutil.copy(lib,path)
(r/'etc/passwd').write_text('root:x:0:0:root:/root:/bin/sh\n');(r/'etc/shadow').write_text('root::19000:0:99999:7:::\n');(r/'etc/group').write_text('root:x:0:\n');(r/'etc/shells').write_text('/bin/sh\n')
for key in ['vm-key','vm-key.pub']:
 (p/key).unlink(missing_ok=True)
subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(p/'vm-key')],check=True)
shutil.copy(p/'vm-key.pub',r/'root/.ssh/authorized_keys');(r/'root/.ssh').chmod(0o700);(r/'root/.ssh/authorized_keys').chmod(0o600)
subprocess.run(['dropbearkey','-t','ed25519','-f',str(r/'etc/dropbear/dropbear_ed25519_host_key')],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
subprocess.run(['dropbearkey','-t','rsa','-s','2048','-f',str(r/'etc/dropbear/dropbear_rsa_host_key')],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
# Ubuntu generic kernel builds virtio modules separately; use built-in e1000
# if present, otherwise include its module and dependency modules.
kernel=sorted(pathlib.Path('/boot').glob('vmlinuz-*-generic'))[-1];version=kernel.name.removeprefix('vmlinuz-')
mods=subprocess.check_output(['modprobe','--show-depends','-S',version,'e1000']).decode()
modpaths=re.findall(r'insmod (\S+)',mods)
plain=[]
for mod in modpaths:
 name=mod.removesuffix('.zst');path=r/name.lstrip('/');path.parent.mkdir(parents=True,exist_ok=True)
 if mod.endswith('.zst'):
  with path.open('wb') as out:subprocess.run(['zstd','-d','-c',mod],stdout=out,check=True)
 else:shutil.copy(mod,path)
 plain.append(name)
ins='\n'.join('insmod '+mod for mod in plain)
init='''#!/bin/sh
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev
hostname r45-clock-vm
'''+ins+'''
ip link set lo up
ip link set eth0 up
ip addr add 10.0.2.15/24 dev eth0
ip route add default via 10.0.2.2
/sbin/dropbear -E -s -r /etc/dropbear/dropbear_ed25519_host_key -r /etc/dropbear/dropbear_rsa_host_key
printf 'R45_VM_READY\\n'
while true; do sleep 60; done
'''
(r/'init').write_text(init);(r/'init').chmod(0o755)
with (p/'initramfs.gz').open('wb') as out:
 a=subprocess.Popen(['find','.','-print0'],cwd=r,stdout=subprocess.PIPE);b=subprocess.Popen(['cpio','--null','-o','--format=newc'],cwd=r,stdin=a.stdout,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL);a.stdout.close();subprocess.run(['gzip','-1'],stdin=b.stdout,stdout=out,check=True);b.stdout.close();a.wait();b.wait()
args=['qemu-system-x86_64','-enable-kvm','-cpu','host','-m','384','-smp','1','-kernel',str(kernel),'-initrd',str(p/'initramfs.gz'),'-append','console=ttyS0 rdinit=/init panic=1','-nographic','-no-reboot','-nic','user,model=e1000,hostfwd=tcp:127.0.0.1:22745-:22','-pidfile',str(p/'qemu.pid'),'-daemonize','-serial','file:'+str(p/'vm-console.log'),'-monitor','none','-display','none']
# daemonize is incompatible with -nographic; explicit display/serial are enough.
args.remove('-nographic');subprocess.run(args,check=True);print('KVM clock VM started with isolated guest clock')
