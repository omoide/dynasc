#!/usr/bin/env sh
sudo apt-get update;

# Cobra
go install github.com/spf13/cobra-cli@v1.3.0;

# Go Releaser
go install github.com/goreleaser/goreleaser@latest