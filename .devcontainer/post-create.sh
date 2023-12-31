#!/usr/bin/env sh
sudo apt-get update;

# AWS SAM
curl -o /tmp/aws-sam-cli-linux-x86_64.zip -L https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip;
unzip /tmp/aws-sam-cli-linux-x86_64.zip -d /tmp/sam;
sudo /tmp/sam/install;
rm -rf /tmp/aws-sam-cli-linux-x86_64.zip /tmp/sam;

# Cobra
go install github.com/spf13/cobra-cli@v1.3.0;

# Go Releaser
go install github.com/goreleaser/goreleaser@latest