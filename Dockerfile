FROM golang:1.11 as build
# Add the local files to be sure we are building the local source code instead
# of downloading from GitHub.
# Don't add any of the other libraries, because we live at HEAD.
ENV CGO_ENABLED 0
COPY . /go/src/github.com/m-lab/pusher
RUN go get -v github.com/m-lab/pusher

# Now copy the built binary into a minimal base image.
# For debugging, use Alpine Linux as the base with the image alpine:3.7
# For a stable production environment use gcr.io/distroless/static
# TODO(https://github.com/m-lab/pusher/issues/45):
#   switch the base image to a distroless one once the platform is stable
FROM alpine:3.7
COPY --from=build /go/bin/pusher /
WORKDIR /

# To set the command-line args use their corresponding environment variables or
# add the flags or args to the end of the "docker run measurementlab/pusher"
# command.
ENTRYPOINT ["/pusher"]
