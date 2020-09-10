import pymysql.cursors
import os
import signal
import subprocess

class TiUpManager(object):

    def setup(self):
        cmd = "tiup playground >/tmp/tiup_log.txt 2>&1 &"
        subprocess.Popen(cmd, shell=True)

    def scale_in(self, instance_type, num=0):
        cmd = "tiup playground scale-in --pid {pid}"
        pids = self.get_pids_for_type(instance_type)
        for i in range(num, len(pids)):
            line = cmd.format(pid=pids[i].decode("utf-8"))
            out = subprocess.Popen(line, shell=True)

    def scale_out(self):
        cmd = "tiup playground scale-out --db 1"
        out = subprocess.Popen(cmd, shell=True)

    def get_pids_for_type(self, instance_type):
        out = subprocess.Popen(['tiup', 'playground', 'display'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = out.communicate()
        if stderr:
            return {}

        return self.parse(stdout, instance_type.encode())

    def parse(self, stdout, instance_type):
        result = []
        strs = stdout.split(b'\n')
        for str in strs:
            words = str.split(b' ')
            if len(words) == 0:
                continue
            pid = words[0]
            for word in words:
                if word == instance_type:
                    result.append(pid)
                    continue
        return result

    def close(self):
        pgid = os.getpgid(os.getpid())
        os.killpg(pgid, signal.SIGTERM)