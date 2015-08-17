FROM golang:latest
RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN go get github.com/tools/godep
RUN godep go build -o presto_metrico .
CMD ["/app/presto_metrico"]
