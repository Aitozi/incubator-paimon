#!/bin/bash

# source ~/.zshrc
# 循环执行10次
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
for i in {1..10}; do
    mvn -s ~/.m2/settings-central.xml test -Dtest=org.apache.paimon.operation.OrphanFilesCleanTest
done
