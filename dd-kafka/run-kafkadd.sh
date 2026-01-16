mvn clean -DskipTests package && docker compose -f docker-compose.yml up --build
mongosh "mongodb://localhost:27018" --eval "db.runCommand({ ping: 1 })"