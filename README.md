# tutorcast-distributed-cache

Distributed cache server for Tutorcast. Writting in Java.

Uses Finagle (Netty based container) for asynchronous http communication via Thrift

Store received events to a collection of Redis instances

Once a session is complete flush all events to S3
