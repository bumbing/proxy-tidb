import pymysql.cursors
import threading
import tiup_manger
from readerwriterlock import rwlock

MINUTES_IDLE_TO_CLOSE_TIDB = 3
SECONDS_IN_A_MINUTE = 60


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

    def __init__(self):
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
            if self.idle_period < MINUTES_IDLE_TO_CLOSE_TIDB:
                self.idle_period += 1
            else:
                self.__scale_in_if_idle()
        else:
            self.idle_period = 0
        threading.Timer(SECONDS_IN_A_MINUTE, self.__periodically_check).start()

    def __establish_conn(self):
        if not self.is_db_instantiated:
            l = self.lock.gen_wlock()
            if l.acquire():
                tiup_manager = tiup_manger.TiUpManager.getInstance()
                tiup_manager.scale_out()
                self.is_db_instantiated = True
                self.connection_pool.clear()
            l.release()

        connection = pymysql.connect(host='127.0.0.1', user='root', port=4000, charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        self.connection_pool.append(connection)
