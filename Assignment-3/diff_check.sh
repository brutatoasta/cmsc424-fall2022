#!/bin/sh

db=$1
eg=$2
javac MetaData.java
java -classpath .:./postgresql-42.2.10.jar MetaData stackexchange > OutputMetadataStackexchange.txt

diff OutputMetadataStackexchange.txt $2 > diff.txt
if [ $? -eq 0 ]; then
	echo "Files are the same"
fi
