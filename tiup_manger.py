import os
import signal
import subprocess


class TiUpManager(object):
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if TiUpManager.__instance is None:
            TiUpManager()
        return TiUpManager.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if TiUpManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            TiUpManager.__instance = self
            self.__setup()

    def __setup(self):
        cmd = "tiup playground >/tmp/tiup_log.txt 2>&1 &"
        subprocess.call(cmd, shell=True)

    def scale_in(self, instance_type, num=0):
        cmd = "tiup playground scale-in --pid {pid}"
        pids = self.__get_pids_for_type(instance_type)
        for i in range(num, len(pids)):
            line = cmd.format(pid=pids[i].decode("utf-8"))
            subprocess.call(line, shell=True)

    def scale_out(self):
        cmd = "tiup playground scale-out --db 1"
        subprocess.call(cmd, shell=True)

    def __get_pids_for_type(self, instance_type):
        out = subprocess.Popen(['tiup', 'playground', 'display'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = out.communicate()
        if stderr:
            return {}

        return self.__parse(stdout, instance_type.encode())

    def __parse(self, stdout, instance_type):
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Kill all process associated with current pgid, containing all tidb threads
        pgid = os.getpgid(os.getpid())
        os.killpg(pgid, signal.SIGTERM)
