# dynasc

A cross-platform client tool for DynamoDB Streams written in Go.

[![Release](https://img.shields.io/github/release/omoidecom/dynasc.svg?label=Release)](https://github.com/omoidecom/dynasc/releases)
[![GitHub Actions](https://github.com/omoidecom/dynasc/actions/workflows/test.yaml/badge.svg)](https://github.com/omoidecom/dynasc/actions?query=branch%3Amaster+workflow%3Atest)
[![Go Report Card](https://goreportcard.com/badge/github.com/omoidecom/dynasc)](https://goreportcard.com/report/github.com/omoidecom/dynasc)
[![GoDoc](https://godoc.org/github.com/omoidecom/dynasc?status.svg)](https://godoc.org/github.com/omoidecom/dynasc)
[![Docker Pulls](https://img.shields.io/docker/pulls/omoide/dynasc.svg)](https://hub.docker.com/r/omoide/dynasc/)

## Overview

Dynasc (**Dyna**moDB **S**treams **C**lient) is a client tool for processing stream records written by DynamoDB Streams.

Dynasc reads the stream records from the any shards in DynamoDB Streams and invokes any lambda functions with the payloads that contain the DynamoDB Streams record event.
The primary use case is to emulate the integration of Amazon DynamoDB and AWS Lambda in your own development environment while combining it with tools such as [DynamoDB local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) and [AWS SAM](https://aws.amazon.com/jp/serverless/sam/).

> [!NOTE]  
> Do not use this tool in a production environment.
> You can create triggers to integrate Amazon DynamoDB with AWS Lambda without this tool.

## Installing

### Pre-Build binaries

Prebuilt binaries are available for a variety of operating systems and architectures.
Visit the [latest release](https://github.com/omoidecom/dynasc/releases/latest) page, and scroll down to the Assets section.

1. Download the archive for the desired operating system and architecture.
2. Extract the archive.
3. Move the executable to the desired directory.
4. Add this directory to the PATH environment variable.
5. Verify that you have execute permission on the file.

Please consult your operating system documentation if you need help setting file permissions or modifying your PATH environment variable.

If you do not see a prebuilt binary for the desired operating system and architecture, install Dynasc using one of the methods described below.

### Build from source

To build from source you must:

1. Install [Git](https://git-scm.com/).
2. Install [Go](https://go.dev/) version 1.21 or later.

Then build and test:

```
go install github.com/omoidecom/dynasc/cmd/dynasc@latest
dynasc -v
```

## Usage

To be written.

## License

Dynasc is released under the MIT license. See [LICENSE](LICENSE).
