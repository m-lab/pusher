FROM golang:1.10 as build
# Add the local files to be sure we are building the local source code instead of downloading from GitHub.
# Don't add any of the other libraries, because we live at HEAD.
ADD . /go/src/github.com/m-lab/pusher
RUN go get -v github.com/m-lab/pusher

# Now copy the built binary into a minimal base image.
FROM gcr.io/distroless/base
COPY --from=build /go/bin/pusher /
WORKDIR /

# To set the command-line args use their corresponding environment variables.
CMD ["/pusher"]
