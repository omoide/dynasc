#!/usr/bin/env sh
sudo apt-get update;

# AWS CLI
curl -o "/tmp/awscli-exe-linux-x86_64.zip" -L "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
unzip /tmp/awscli-exe-linux-x86_64.zip -d /tmp;
sudo /tmp/aws/install;
rm -rf /tmp/awscli-exe-linux-x86_64.zip /tmp/aws;

# AWS SAM
curl -o /tmp/aws-sam-cli-linux-x86_64.zip -L https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip;
unzip /tmp/aws-sam-cli-linux-x86_64.zip -d /tmp/sam;
sudo /tmp/sam/install;
rm -rf /tmp/aws-sam-cli-linux-x86_64.zip /tmp/sam;