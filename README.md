# dynasc

A cross-platform client tool for DynamoDB Streams written in Go.

[![Release](https://img.shields.io/github/release/omoide/dynasc.svg?label=Release)](https://github.com/omoide/dynasc/releases)
[![GitHub Actions](https://github.com/omoide/dynasc/actions/workflows/test.yaml/badge.svg)](https://github.com/omoide/dynasc/actions?query=branch%3Amaster+workflow%3Atest)
[![Go Report Card](https://goreportcard.com/badge/github.com/omoide/dynasc)](https://goreportcard.com/report/github.com/omoide/dynasc)
[![GoDoc](https://godoc.org/github.com/omoide/dynasc?status.svg)](https://godoc.org/github.com/omoide/dynasc)
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
Visit the [latest release](https://github.com/omoide/dynasc/releases/latest) page, and scroll down to the Assets section.

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
go install github.com/omoide/dynasc/cmd/dynasc@latest
dynasc -v
```

## Usage

Start a client:

```
dynasc --dynamodb-endpoint http://localhost:8000 --lambda-endpoint http://localhost:3000 --triggers TableA=Function1,TableB=Function2
```

You can configure Dynasc in in several ways, such as system or user environment variables, a local configuration file, or explicitly declared command line options.  
Configuration settings take precedence in the following order. Each item takes precedence over the item below it:

1. command line options
2. environment variables
3. configuration file

### Command line options

You can use the following command line options to override any configuration file setting, or environment variable setting.

| Option                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| --dynamo-endpoint _\<string\>_ | Specifies an Amazon DynamoDB endpoint to read stream records.<br />You need to specify this endpoint, if you are emulating Amazon DynamoDB in local using tools such as [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) or [Local Stack](https://docs.localstack.cloud/user-guide/aws/dynamodb/).<br /> If this option is not specified, the standard service endpoint is used automatically based on the specified AWS Region.                                                                |
| --lambda-endpoint _\<string\>_ | Specifies an AWS Lambda endpoint to execute functions.<br />You need to specify this endpoint, if you are emulating AWS Lambda in local using tools such as [AWS SAM CLI](https://github.com/aws/aws-sam-cli).<br /> If this option is not specified, the standard service endpoint is used automatically based on the specified AWS Region.                                                                                                                                                                                                         |
| --triggers _\<string\>_        | Specifies trigger definition consisting of a set of key-value pairs of Amazon DynamoDB table names and AWS Lambda function names.<br /><br />Multiple key-value pairs are separated by commas, for example: `TableA=Function1,TableB=Function2`.<br />You can also define multiple functions for the same table, for example: `TableA=Function1,TableA=Function2`.<br /><br />Based on this definition, Dynasc polls shards in the streams of the specified tables. When stream records are available, Dynasc invokes corresponding Lambda function. |
| --config _\<string\>_          | Specifies a path and file name of the configuration file containing default parameter values to use.<br /> For more information about configuration files, see [configuration file](#configuration-file).                                                                                                                                                                                                                                                                                                                                            |
| -d, --debug                    | A Boolean switch that enables debug logging.<br />The --debug option provides the full Go logs. This includes additional stderr diagnostic information about the operation of the command that can be useful when troubleshooting why a command provides unexpected results.                                                                                                                                                                                                                                                                         |
| -h, --help                     | A Boolean switch that displays help for the options.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -v, --version                  | A Boolean switch that displays the current version of Dynasc.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

### Ennvironment variables

Environment variables provide another way to specify configuration options.  
Dynasc supports the following environment variables.

| Option                 | Description                                                                                                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AWS_REGION             | Specifies the AWS Region to send the request to.                                                                                                                              |
| AWS_ACCESS_KEY_ID      | Specifies an AWS access key associated with an IAM account.                                                                                                                   |
| AWS_SECRET_ACCESS_KEY  | Specifies the secret key associated with the access key.                                                                                                                      |
| DYNASC_DYNAMO_ENDPOINT | Same as the command line `--dynamo-endpoint` option.                                                                                                                          |
| DYNASC_LAMBDA_ENDPOINT | Same as the command line `--lambad-endpoint` option.                                                                                                                          |
| DYNASC_TRIGGERS        | Same as the command line `--triggers` option.<br />However, multiple key-value par must be separated by spaces, not commas, for example: `TableA=Function1 TableB=Function2`. |
| DYNASC_CONFIG          | Same as the command line `--config` option.                                                                                                                                   |

If you use a local DynamoDB or Lambda that cares about credentials, you will specify `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

### Configurationn file

Dynasc supports a configuration file that you can use to configure Dynasc command parameter values.  
Supported formats are `JSON`, `YAML`, `TOML`, and `HCL`.

Samples of condfiguration file are as follows.

#### JSON

```json
{
  "dynamo-endpoint": "http://localhost:8000",
  "lambda-endpoint": "http://localhost:3001",
  "triggers": [
    { "table": "Table1", "functions": ["Function1", "Function2"] },
    { "table": "Table2", "functions": "Function1" }
  ]
}
```

#### YAML

```yaml
dynamo-endpoint: http://localhost:8000
lambda-endpoint: http://localhost:3001
triggers:
  - table: Table1
    functions:
      - Function1
      - Function2
  - table: Table2
    functions: Function1
```

As you can see from the samples above, `functions` element of trigger can be specified as `string` or `array of strings`.

## License

Dynasc is released under the MIT license. See [LICENSE](LICENSE).
