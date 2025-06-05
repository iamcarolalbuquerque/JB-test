# JB-test

## Export environment variables
export POSTGRES_PASSWORD=XXXXXX
## Execute postgres docker image localy:
docker pull postgres

docker run --name some-postgres -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -p 5432:5432 --security-opt seccomp=unconfined -d postgres