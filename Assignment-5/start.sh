#!/bin/bash

systemctl start mongod.service
mongoimport --db "analytics" --collection "customers" sample_analytics/customers.json
mongoimport --db "analytics" --collection "accounts" sample_analytics/accounts.json
mongoimport --db "analytics" --collection "transactions" sample_analytics/transactions.json