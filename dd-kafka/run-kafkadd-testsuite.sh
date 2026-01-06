mvn -DskipTests package && docker compose -f docker-compose-test.yml up --build
mongosh "mongodb://localhost:27018" --eval "db.runCommand({ ping: 1 })"