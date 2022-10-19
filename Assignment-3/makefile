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
tdash:
	curl -X GET 127.0.0.1:5000/dashboard/top100users

tp:
	curl -X POST -d '{"reputation": 1, "creationdate": "2022-10-01", "displayname": "something", "upvotes": 10, "downvotes": 0}' -H 'Content-Type: application/json'  127.0.0.1:5000/user/11111