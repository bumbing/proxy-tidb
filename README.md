# proxy-tidb

## Example:

```text
(venv) (base) ➜  proxy-tidb git:(master) ✗ python3 interactive_cli.py
Starting component `playground`:  scale-out --db 1
Error: Post http://127.0.0.1:9527/command: dial tcp 127.0.0.1:9527: connect: connection refused
Error: run `` (wd:/Users/bumbing/.tiup/data/SAATuWs) failed: exit status 1
waiting for mysql port ready!
waiting for mysql port ready!
waiting for mysql port ready!
(Cmd) help

Documented commands (use 'help -v' for verbose/'help <topic>' for details):
===========================================================================
alias  help     macro  quit          run_script  shell    
edit   history  py     run_pyscript  set         shortcuts

Undocumented commands:
======================
query_tidb

(Cmd) query_tidb -s 'use test'
sql: use test 
result: ()
(Cmd) query_tidb -s 'show tables'
sql: show tables 
result: ()
(Cmd) query_tidb -s 'CREATE TABLE IF NOT EXISTS test (id INT)'
sql: CREATE TABLE IF NOT EXISTS test (id INT) 
result: ()
(Cmd) query_tidb -s 'INSERT INTO `test` VALUES (1)'
sql: INSERT INTO `test` VALUES (1) 
result: ()
(Cmd) query_tidb -s 'SELECT * FROM test'
sql: SELECT * FROM test 
result: ((1,),)
```
