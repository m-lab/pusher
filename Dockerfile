FROM golang:1.20 as build
# Add the local files to be sure we are building the local source code instead
# of downloading from GitHub.
# Don't add any of the other libraries, because we live at HEAD.
ENV CGO_ENABLED 0
COPY . /go/src/github.com/m-lab/pusher
WORKDIR /go/src/github.com/m-lab/pusher

# Build pusher and put the git commit hash into the binary.
RUN go install \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)$(git diff --quiet || echo dirty)" \
      github.com/m-lab/pusher

# Now copy the built binary into a minimal base image.
FROM alpine:3.15
# By default, alpine has no root certs. Add them so pusher can use PKI to
# verify that Google Cloud Storage is actually Google Cloud Storage.
RUN apk add --no-cache ca-certificates

COPY --from=build /go/bin/pusher /
WORKDIR /
# Make sure /pusher can run (has no missing external dependencies).
RUN /pusher -h 2> /dev/null
# To set the command-line args use their corresponding environment variables or
# add the flags or args to the end of the "docker run measurementlab/pusher"
# command.
ENTRYPOINT ["/pusher"]
