#!/usr/bin/env bash
java -jar ./target/scala-2.11/diffy-server.jar \
-candidate='192.168.99.100:9881' \
-master.primary='192.168.99.100:9882' \
-master.secondary='192.168.99.100:9883' \
-service.protocol='http' \
-serviceName='My Service' \
-proxy.port=:8880 \
-admin.port=:8881 \
-http.port=:8888 \
-rootUrl='localhost:8888'

