import argparse
import random

import cmd2

import connection_manager


class InteractiveCli(cmd2.Cmd):
    conn = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        cm = connection_manager.ConnectionManager.getInstance()
        self.conn = cm.offer_conn()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--sql', type=str, help='query to execute')

    @cmd2.with_argparser(parser)
    def do_query_tidb(self, args):
        with self.conn.cursor() as cursor:
            cursor.execute(args.sql)
            self.conn.commit()
            result = cursor.fetchall()
        self.poutput('sql: {} \nresult: {}'.format(args.sql, result))


if __name__ == '__main__':
    import sys
    c = InteractiveCli()
    sys.exit(c.cmdloop())
