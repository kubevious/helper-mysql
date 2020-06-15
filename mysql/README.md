## RUN Locally with Docker Compose
docker-compose up -d --build

docker-compose kill
docker-compose rm -f

docker-compose up --force-recreate


## RUN Locally Manually
docker stop mysql; docker rm mysql; docker run --rm --name mysql -p 3306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -d mysql --default-authentication-plugin=mysql_native_password


## Attach Remote
kubectl port-forward $(kubectl get pod -l k8s-app=kubevious-mysql -n kubevious -o jsonpath="{.items[0].metadata.name}") 3306:3306 -n kubevious