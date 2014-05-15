# ZeroMQ UDF for MariaDB/MySQL

## Build

required.

* [cmake](http://www.cmake.org)
* [ZeroMQ](http://zeromq.org)

```
% mkdir build && cd build
% cmake -DCMAKE_BUILD_TYPE=Release ..
% make
% make install
```

Create Function MariaDB/MySQL UDF.

```
mariadb> CREATE FUNCTION zmq_push RETURNS INTEGER SONAME 'udf_zmq.so';
mariadb> CREATE FUNCTION zmq_pub RETURNS INTEGER SONAME 'udf_zmq.so';
mariadb> CREATE FUNCTION zmq_req RETURNS STRING SONAME 'udf_zmq.so';
mariadb> CREATE FUNCTION zmq_stream RETURNS STRING SONAME 'udf_zmq.so';
mariadb> CREATE FUNCTION zmq_serialize RETURNS STRING SONAME 'udf_zmq.so';
```

<!--
mariadb> SELECT * FROM `mysql`.`func`;
+---------------+-----+------------+----------+
| name          | ret | dl         | type     |
+---------------+-----+------------+----------+
| zmq_push      |   2 | udf_zmq.so | function |
| zmq_pub       |   2 | udf_zmq.so | function |
| zmq_req       |   0 | udf_zmq.so | function |
| zmq_stream    |   0 | udf_zmq.so | function |
| zmq_serialize |   0 | udf_zmq.so | function |
+---------------+-----+------------+----------+
-->

## Function

### zmq\_push : ZeroMQ PUSH sender

integer **zmq\_push** ( string _ENDPOINT_ , string _MESSAGE_ [ , MESSAGE, ... ] )

```
mariadb> SELECT zmq_push('tcp://localhost:5555', 'Message') AS zmq;
+-----+
| zmq |
+-----+
|   0 |
+-----+
```

### zmq\_pub : ZeroMQ PUB sender

integer **zmq\_pub** ( string _ENDPOINT_ , string _MESSAGE_ [ , MESSAGE, ... ] )

```
mariadb> SELECT zmq_pub('tcp://localhost:5555', 'Subscribe', 'Message') AS zmq;
+-----+
| zmq |
+-----+
|   0 |
+-----+
```

### zmq\_req : ZeroMQ REQ sender

string **zmq\_req** ( string _ENDPOINT_ , string _MESSAGE_ [ , MESSAGE, ... ] )

```
mariadb> SELECT zmq_req('tcp://localhost:5555', 'Message') AS zmq;
+---------------+
| zmq           |
+---------------+
| Reply Message |
+---------------+
```

### zmq\_stream : ZeroMQ STREAM sender

integer **zmq\_stream** ( string _ENDPOINT_ , string _MESSAGE_ [ , MESSAGE, ... ] )

```
mariadb> SELECT zmq_stream('tcp://localhost:5555', 'Message') AS zmq;
+------------------+
| zmq              |
+------------------+
| Response Message |
+------------------+
```

<!--
SELECT zmq_stream('tcp://127.0.0.1:80', "GET / HTTP/1.1\r\nHost:localhost\r\n\r\n");
SELECT zmq_stream('tcp://127.0.0.1:80', "POST / HTTP/1.1\r\nHost:localhost\r\nContent-Type:application/x-www-form-urlencoded\r\nContent-Length:9\r\n\r\ntest=hoge\r\n") as zmq;
-->
