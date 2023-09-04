# dynasc

A cross-platform client tool for DynamoDB Streams written in Go.

[![GitHub Actions](https://github.com/omoidecom/dynasc/actions/workflows/test.yaml/badge.svg)](https://github.com/omoidecom/dynasc/actions?query=branch%3Amaster+workflow%3Atest)

## Overview

Dynasc (**Dyna**moDB **S**treams **C**lient) is a client tool for processing stream records written by DynamoDB Streams.

Dynasc reads the stream records from the any shards in DynamoDB Streams and invokes any lambda functions with the payloads that contain the DynamoDB Streams record event.
The primary use case is to emulate the integration of Amazon DynamoDB and AWS Lambda in your own development environment while combining it with tools such as [DynamoDB local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) and [AWS SAM](https://aws.amazon.com/jp/serverless/sam/).

> [!NOTE]  
> Do not use this tool in a production environment.
> You can create triggers to integrate Amazon DynamoDB with AWS Lambda without this tool.

## Installing

To be written.

## Usage

To be written.

## License

Dynasc is released under the MIT license. See [LICENSE](LICENSE).
