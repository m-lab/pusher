FROM golang:1.11 as build
# Add the local files to be sure we are building the local source code instead
# of downloading from GitHub.
# Don't add any of the other libraries, because we live at HEAD.
ENV CGO_ENABLED 0
COPY . /go/src/github.com/m-lab/pusher
RUN go get -v github.com/m-lab/pusher

# Now copy the built binary into a minimal base image.
FROM alpine:3.7
# By default, alpine has no root certs. Add them so pusher can use PKI to
# verify that Google Cloud Storage is actually Google Cloud Storage.
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/pusher /
WORKDIR /

# To set the command-line args use their corresponding environment variables or
# add the flags or args to the end of the "docker run measurementlab/pusher"
# command.
ENTRYPOINT ["/pusher"]
