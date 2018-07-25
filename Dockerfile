FROM golang:1.10 as build
ADD . /go/src/github.com/m-lab/pusher
RUN go get -v github.com/m-lab/pusher

# Now copy the built binary into a minimal base image
FROM gcr.io/distroless/base
COPY --from=build /go/bin/pusher /
WORKDIR /

# To set the command-line args use their corresponding environment variables.
CMD ["/pusher"]
