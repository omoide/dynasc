# Build artifacts.
FROM golang:1.21 as build
WORKDIR /work
COPY go.mod go.sum ./
COPY main.go .
RUN CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap main.go

# Copy artifacts to a clean image.
FROM public.ecr.aws/lambda/provided:al2
COPY --from=build /work/bootstrap /var/runtime/bootstrap
ENTRYPOINT [ "/var/runtime/bootstrap" ]