docker version: Docker 19.03.0 

This can be done in 2 ways:

Set the environment variable DOCKER_BUILDKIT=1
Set it in the docker engine by default by adding "features": { "buildkit": true } to the root of the config json.

docker build --file ./build/linux/Dockerfile --output out .