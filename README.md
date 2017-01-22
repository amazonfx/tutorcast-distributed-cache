# tutorcast-distributed-cache

Distributed cache server for Tutorcast. Writting in Java.

Uses Finagle (Netty based container) for asynchronous http communication via Thrift

Stores received events to a collection of Redis instances

Once a sesion is complete flush all events to that session to S3
