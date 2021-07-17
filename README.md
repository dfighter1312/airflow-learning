# Practice with Airflow

This repository is created based on **Learning Apache Airflow with Python in easy way in 40 Minutes**

Link: https://www.youtube.com/watch?v=2v9AKewyUEo

To work:
1. Install Docker/Docker-compose/Docker Desktop and clone the repository.
2. Go to `project` directory
```
$ cd project
```
3. (Optional) Remove all Docker containers and images if you have already installed Docker before.
```
$ docker system prune
$ docker image prune
```
4. (Optional) To run database migrations and create the first user account, run
```
$ docker up airflow-init
```
5. Run the docker-compose.
```
$ docker-compose up --build
```
6. Open `localhost:8080` to see the magic. Beforehand, the username and password for logging in are `airflow`.
7. Before finishing, type the command
```
docker-compose down
```

## Repository updates:

- 1.1. Update Airflow `docker-compose.yml` file.