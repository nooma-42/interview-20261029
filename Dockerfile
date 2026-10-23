FROM golang:1.25.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rockets ./cmd/rockets
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rockets /rockets
EXPOSE 8088
USER 65532:65532
ENTRYPOINT ["/rockets"]
CMD ["serve"]
