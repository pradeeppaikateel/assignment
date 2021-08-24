docker build -t pradeep/spark -f Dockerfile .
docker build -t pradeep/pgadmin4 -f Dockerfile2 .
docker network inspect develop >$null 2>&1 || docker network create --subnet=172.18.0.0/16 develop
docker start postgres > $null 2>&1 || docker run --name postgres --restart unless-stopped -e POSTGRES_PASSWORD=postgres --net=develop --ip 172.18.0.22 -e PGDATA=/var/lib/postgresql/data/pgdata -v /opt/postgres:/var/lib/postgresql/data -p 5432:5432 -d postgres:11
docker-compose up -d
docker run -p 5050:80 --volume=pgadmin4:/var/lib/pgadmin -e PGADMIN_DEFAULT_EMAIL=postman@sample.com -e PGADMIN_SERVER_JSON_FILE=/pgadmin4/servers.json -e PGADMIN_DEFAULT_PASSWORD=postman --name="pgadmin4" --hostname="pgadmin4" --network="develop" --detach pradeep/pgadmin4
