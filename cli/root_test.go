package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestNewRootCmd(t *testing.T) {
	type flag struct {
		name       string
		shorthand  string
		usage      string
		value      any
		persistent bool
	}
	// Define test cases.
	tests := []struct {
		name string
		flag *flag
	}{
		{
			name: "Normal: the help flag is defined",
			flag: &flag{
				name:       "help",
				shorthand:  "h",
				usage:      "Show the help and exit.",
				value:      "false",
				persistent: true,
			},
		},
		{
			name: "Normal: the version flag is defined",
			flag: &flag{
				name:       "version",
				shorthand:  "v",
				usage:      "Show the version and exit.",
				value:      "false",
				persistent: true,
			},
		},
		{
			name: "Normal: the debug flag is defined",
			flag: &flag{
				name:       "debug",
				shorthand:  "d",
				usage:      "Enable debug logging.",
				value:      "false",
				persistent: true,
			},
		},
		{
			name: "Normal: the config flag is defined",
			flag: &flag{
				name:       "config",
				shorthand:  "",
				usage:      "Configuration file containing default parameter values.",
				value:      "",
				persistent: true,
			},
		},
		{
			name: "Normal: the triggers flag is defined",
			flag: &flag{
				name:       "triggers",
				shorthand:  "",
				usage:      "Trigger definition consisting of a set of key-value pairs of Amazon DynamoDB table names and AWS Lambda function names.",
				value:      "[]",
				persistent: false,
			},
		},
		{
			name: "Normal: the dynamo-endpoint flag is defined",
			flag: &flag{
				name:       "dynamo-endpoint",
				shorthand:  "",
				usage:      "Amazon DynamoDB endpoint to read stream records.",
				value:      "",
				persistent: false,
			},
		},
		{
			name: "Normal: the lambda-endpoint flag is defined",
			flag: &flag{
				name:       "lambda-endpoint",
				shorthand:  "",
				usage:      "AWS Lambda endpoint to execute functions.",
				value:      "",
				persistent: false,
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			cmd := NewRootCmd()
			flag := func() *pflag.Flag {
				if tc.flag.persistent {
					return cmd.PersistentFlags().Lookup(tc.flag.name)
				} else {
					return cmd.Flags().Lookup(tc.flag.name)
				}
			}()
			assert.NotNil(t, flag)
			assert.Equal(t, tc.flag.name, flag.Name)
			assert.Equal(t, tc.flag.shorthand, flag.Shorthand)
			assert.Equal(t, tc.flag.usage, flag.Usage)
			assert.Equal(t, tc.flag.value, flag.DefValue)
		})
	}
}

func TestInitLogger(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		cmd func(cmd *cobra.Command) *cobra.Command
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name  string
		in    *in
		out   *out
		level slog.Level
	}{
		{
			name: "Normal: the debug level is enabled",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().Bool("debug", true, "")
					return cmd
				},
			},
			out:   &out{},
			level: slog.LevelDebug,
		},
		{
			name: "Normal: the debug level is not enabled",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().Bool("debug", false, "")
					return cmd
				},
			},
			out:   &out{},
			level: slog.LevelInfo,
		},
		{
			name: "Abnormal: the debug flag does not defined",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					return cmd
				},
			},
			out: &out{
				err: errors.New("failed to get a flag named \"debug\" correctly"),
			},
		},
	}
	// Execute.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			cmd := tc.in.cmd(&cobra.Command{
				Run: func(cmd *cobra.Command, args []string) {
					err := initLogger(cmd)
					if tc.out.err != nil {
						assert.ErrorContains(t, err, tc.out.err.Error())
					} else {
						assert.NoError(t, err)
						assert.True(t, slog.Default().Enabled(context.Background(), tc.level))
						assert.False(t, slog.Default().Enabled(context.Background(), tc.level-1))
					}
				},
			})
			assert.NoError(t, cmd.Execute())
		})
	}
}

func TestInitConfig(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		cmd func(cmd *cobra.Command) *cobra.Command
	}
	type out struct {
		configFile string
		err        error
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: the configuration file is not specified",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "", "")
					return cmd
				},
			},
			out: &out{
				configFile: "",
			},
		},
		{
			name: "Normal: the configuration file is specified",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/config", "")
					return cmd
				},
			},
			out: &out{
				configFile: "../testdata/config/config",
			},
		},
		{
			name: "Abnormal: the specified configuration file does not exist",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/nonexistent", "")
					return cmd
				},
			},
			out: &out{
				err: errors.New("failed to read a configuration file"),
			},
		},
	}
	// Execute.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			cmd := tc.in.cmd(&cobra.Command{
				Run: func(cmd *cobra.Command, args []string) {
					vi, err := initConfig(cmd)
					if tc.out.err != nil {
						assert.ErrorContains(t, err, tc.out.err.Error())
					} else {
						assert.NoError(t, err)
						assert.Equal(t, tc.out.configFile, vi.ConfigFileUsed())
					}
				},
			})
			assert.NoError(t, cmd.Execute())
		})
	}
}

func TestInitRootOption(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		cmd func(cmd *cobra.Command) *cobra.Command
	}
	type out struct {
		opt *RootOption
		err error
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
		env  map[string]string
	}{
		{
			name: "Normal: no option is specified",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "", "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					Triggers: map[string][]string{},
				},
			},
		},
		{
			name: "Normal: options are specified in flags",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "", "")
					cmd.Flags().String("dynamo-endpoint", "http://localhost:8000", "")
					cmd.Flags().String("lambda-endpoint", "http://localhost:3001", "")
					cmd.Flags().StringSlice("triggers", []string{"Table1=Function1", "Table1=Function2", "Table1=Function3", "Table2=Function1"}, "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
		},
		{
			name: "Normal: options are specified in environment variables",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "", "")
					cmd.Flags().String("dynamo-endpoint", "", "")
					cmd.Flags().String("lambda-endpoint", "", "")
					cmd.Flags().StringSlice("triggers", nil, "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
			env: map[string]string{
				"DYNASC_DYNAMO_ENDPOINT": "http://localhost:8000",
				"DYNASC_LAMBDA_ENDPOINT": "http://localhost:3001",
				"DYNASC_TRIGGERS":        "Table1=Function1 Table1=Function2 Table1=Function3 Table2=Function1",
			},
		},
		{
			name: "Normal: options are specified in a configuration file with no extension",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/config", "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
		},
		{
			name: "Normal: options are specified in a YAML configuration file",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/config.yaml", "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
		},
		{
			name: "Normal: options are specified in a JSON configuration file",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/config.json", "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
		},
		{
			name: "Normal: options are specified in a TOML configuration file",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/config.toml", "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
		},
		{
			name: "Normal: options are specified in a HCL configuration file",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/config.hcl", "")
					return cmd
				},
			},
			out: &out{
				opt: &RootOption{
					DynamoEndpoint: "http://localhost:8000",
					LambdaEndpoint: "http://localhost:3001",
					Triggers: map[string][]string{
						"Table1": {"Function1", "Function2", "Function3"},
						"Table2": {"Function1"},
					},
				},
			},
		},
		{
			name: "Abnormal: initConfig returns an error",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "../testdata/config/nonexistent", "")
					return cmd
				},
			},
			out: &out{
				opt: nil,
				err: errors.New("failed to initialize the configuration"),
			},
		},
		{
			name: "Abnormal: the format of the configuration file is invalid",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "", "")
					cmd.Flags().StringSlice("dynamo-endpoint", []string{"http://localhost:8000", "http://localhost:8000"}, "")
					return cmd
				},
			},
			out: &out{
				opt: nil,
				err: errors.New("failed to unmarshal the configuration into an option"),
			},
		},
		{
			name: "Abnormal: the format of the triggers specified in a configuration file is invalid",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					// Create a configuration file.
					f, err := os.CreateTemp("", "go-test.*.json")
					if err != nil {
						panic(err)
					}
					defer func() {
						if err := f.Close(); err != nil {
							panic(err)
						}
					}()
					if _, err := f.Write([]byte("{\"triggers\":\"Table1=Function1\"}")); err != nil {
						panic(err)
					}
					cmd.Flags().String("config", f.Name(), "")
					cmd.PostRunE = func(cmd *cobra.Command, args []string) error {
						return os.Remove(f.Name())
					}
					return cmd
				},
			},
			out: &out{
				opt: nil,
				err: errors.New("failed to unmarshal the triggers"),
			},
		},
		{
			name: "Abnormal: the format of the triggers specified in flags is invalid",
			in: &in{
				cmd: func(cmd *cobra.Command) *cobra.Command {
					cmd.Flags().String("config", "", "")
					cmd.Flags().StringSlice("triggers", []string{"Table1:Function1"}, "")
					return cmd
				},
			},
			out: &out{
				opt: nil,
				err: errors.New("failed to parse the trigger"),
			},
		},
	}
	// Execute.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cmd := tc.in.cmd(&cobra.Command{
				Run: func(cmd *cobra.Command, args []string) {
					option, err := initRootOption(cmd)
					if tc.out.err != nil {
						assert.ErrorContains(t, err, tc.out.err.Error())
					} else {
						assert.NoError(t, err)
						assert.Equal(t, tc.out.opt, option)
					}
				},
			})
			assert.NoError(t, cmd.Execute())
		})
	}
}
