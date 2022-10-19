all: eg

eg:
	javac JDBCExample.java
	java -classpath .:./postgresql-42.2.10.jar JDBCExample

j:
	javac NearestNeighbor.java
	java -classpath .:./postgresql-42.2.10.jar NearestNeighbor

flask:
	python3 rest.py
td:
	curl -X DELETE http://127.0.0.1:5000/user/11 