all: build run

# Build docker image
build:
	docker build --rm -t uspto-image .
# Run container in detached mode with the above image. Can be set to automatically be removed when it stops with --rm (left out for now to make sure it works)
run:
	docker run \
	--name uspto-refresh \
	-d \
	uspto-image