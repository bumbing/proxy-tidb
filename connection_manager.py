import pymysql.cursors
import threading
import socket
import tiup_manger
import time
from readerwriterlock import rwlock

IDLE_TIMES_TO_SCALE_IN = 3
SECONDS_TO_CHECK = 60
TIDB_PORT = 4000
LOCAL_HOST = "0.0.0.0"


# ConnectionManager maintains a connection pool to TiDB Servers. If a request comes in, it will check if there are idle
# connections and try to reuse.
# If no connection is used for three mins, it will close them all up, and scale in TiDB server to 0. Then when new
# requests comes in, it scale out and re-establish new connections.
class ConnectionManager(object):
    # lock is used to protest connections being offered when the tidb server is shutting down
    lock = rwlock.RWLockFairD()
    is_db_instantiated = False

    connection_pool = []
    connection_in_use = 0
    idle_period = -1
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if ConnectionManager.__instance is None:
            ConnectionManager()
        return ConnectionManager.__instance

    def __init__(self):
        if ConnectionManager.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            ConnectionManager.__instance = self
            self.__periodically_check()

    def offer_conn(self):
        if len(self.connection_pool) == 0:
            self.__establish_conn()
        conn = self.connection_pool.pop()
        self.connection_in_use += 1
        return conn

    def return_conn(self, conn):
        self.connection_in_use -= 1
        self.connection_pool.append(conn)

    def __scale_in_if_idle(self):
        l = self.lock.gen_wlock()
        if l.acquire():
            for conn in self.connection_pool:
                conn.close()
            self.connection_pool.clear()
            tiup_manager = tiup_manger.TiUpManager.getInstance()
            tiup_manager.scale_in('tidb')
            self.is_db_instantiated = False
        l.release()

    def __periodically_check(self):
        if self.connection_in_use == 0:
            if self.idle_period < IDLE_TIMES_TO_SCALE_IN:
                self.idle_period += 1
            else:
                self.__scale_in_if_idle()
        else:
            self.idle_period = 0
        threading.Timer(SECONDS_TO_CHECK, self.__periodically_check).start()

    def __establish_conn(self):
        if not self.is_db_instantiated:
            l = self.lock.gen_wlock()
            if l.acquire():
                tiup_manager = tiup_manger.TiUpManager.getInstance()
                tiup_manager.scale_out()
                self.is_db_instantiated = True
                self.connection_pool.clear()
            l.release()

        self.__wait_for_port_ready()
        connection = pymysql.connect(host=LOCAL_HOST, user='root', port=TIDB_PORT)
        self.connection_pool.append(connection)

    def __wait_for_port_ready(self):
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex((LOCAL_HOST, TIDB_PORT)) != 0:
                time.sleep(2)
                print("waiting for mysql port ready!")
                continue
            sock.close()
            return

def main():
    #  todo: move to test
    connection_manager = ConnectionManager.getInstance()
    conn = connection_manager.offer_conn()
    with conn.cursor() as cursor:
        # Create a new record
        sql = "USE test"
        cursor.execute(sql)
        conn.commit()
    print("sql 1")

    with conn.cursor() as cursor:
        # Create a new record
        sql = "CREATE TABLE IF NOT EXISTS test (id INT)"
        cursor.execute(sql)
        conn.commit()
    print("sql 2")

    with conn.cursor() as cursor:
        # Read a single record
        sql = "INSERT INTO `test` VALUES (1)"
        cursor.execute(sql)
        conn.commit()
    print("sql 3")

    with conn.cursor() as cursor:
        # Read a single record
        sql = "SELECT * FROM test"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(result)
    print("sql 4")

    connection_manager.return_conn(conn)


if __name__ == "__main__":
    main()