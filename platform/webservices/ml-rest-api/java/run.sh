#!/bin/bash
mvn package
CLASSPATH=`python classpath.py`
pushd target/classes/
java -Djava.util.logging.config.file=../../logging.properties -cp "$CLASSPATH" com/example/Main
popd
