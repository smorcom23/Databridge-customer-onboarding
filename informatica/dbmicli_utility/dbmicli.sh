#!/bin/bash
function ctrl_c() {
    echo ""
}
trap ctrl_c INT
JAVAVER=""
JAVA_VER_MAJOR=""
JAVA_VER_MINOR=""

JAVAVER=$(java -version 2>&1 | grep -i version | awk '{print $3}' | tr -d \")

if [ -z "$JAVAVER" ]; then
    echo Java Runtime Environment is not installed or not configured properly.
    exit $?
fi

for token in $JAVAVER
do
    if [[ $token =~ ([0-9]+)\.([0-9]+)\.(.*) ]]
    then
        JAVA_VER_MAJOR=${BASH_REMATCH[1]}
        JAVA_VER_MINOR=${BASH_REMATCH[2]}
        break
    fi
done

if [ "$JAVA_VER_MAJOR" -eq "1" ] && [ "$JAVA_VER_MINOR" -lt "8" ]; then
    echo Mass Ingestion Databases CLI requires Java Runtime Environment 1.8 or later. The installed JRE version is $JAVAVER.
	exit $?
fi

java -Dlog4j2.formatMsgNoLookups=true -jar dbmicli.jar "$@" 2>>error.log
exit $?