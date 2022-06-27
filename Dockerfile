FROM gcr.io/distroless/static

ADD gh-archived /usr/local/bin/gh-archived

ENTRYPOINT ["gh-archived"]
