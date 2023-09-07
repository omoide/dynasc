package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/omoidecom/dynasc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// App version embedded at the build by ldflags.
var version string

// RootOption represents options for the root command.
type RootOption struct {
	Triggers       map[string][]string `mapstructure:"-"`
	DynamoEndpoint string              `mapstructure:"dynamo-endpoint"`
	LambdaEndpoint string              `mapstructure:"lambda-endpoint"`
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(ctx context.Context) {
	err := NewRootCmd().ExecuteContext(ctx)
	if err != nil {
		slog.Error(err.Error())
		// Output stack trace if the log level is debug.
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			fmt.Printf("%+v\n", err)
		} else {
			fmt.Println("Run the command with the debug option (--debug or -d), if you need detailed logs.")
		}
		os.Exit(1)
	}
}

// NewRootCmd returns an instance of the root command.
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "dynasc",
		Long:          "Dynasc is a client tool for processing stream records \nwritten by DynamoDB Streams.\n\nThis tool reads the stream records from the any shards in \nDynamoDB Streams and invokes any lambda functions with the \npayloads that contain the DynamoDB Streams record event.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Initialize a logger.
			if err := initLogger(cmd); err != nil {
				return errors.Wrap(err, "failed to initialize a logger")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			opt, err := initRootOption(cmd)
			if err != nil {
				return errors.Wrap(err, "failed to initialize the option for the root command")
			}
			return rootRun(cmd.Context(), opt)
		},
	}
	// Define the global flags.
	cmd.PersistentFlags().BoolP("help", "h", false, "Show the help and exit.")
	cmd.PersistentFlags().BoolP("version", "v", false, "Show the version and exit.")
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging.")
	cmd.PersistentFlags().String("config", "", "Configuration file containing default parameter values.")
	// Define the local flags.
	cmd.Flags().StringSlice("triggers", []string{}, "Trigger definition consisting of a set of key-value pairs of Amazon DynamoDB table names and AWS Lambda function names.")
	cmd.Flags().String("dynamo-endpoint", "", "Amazon DynamoDB endpoint to read stream records.")
	cmd.Flags().String("lambda-endpoint", "", "AWS Lambda endpoint to execute functions.")
	return cmd
}

func initLogger(cmd *cobra.Command) error {
	// Get whether debug is enabled or not.
	enableDebug, err := cmd.Flags().GetBool("debug")
	if err != nil {
		return errors.Wrap(err, "failed to get a flag named \"debug\" correctly")
	}
	// Set the default logger.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: func() slog.Level {
			if enableDebug {
				return slog.LevelDebug
			}
			return slog.LevelInfo
		}(),
	})))
	return nil
}

// initRootConfig initialize the configuration for all commands.
func initConfig(cmd *cobra.Command) (*viper.Viper, error) {
	vi := viper.New()
	// Bind the flags.
	if err := vi.BindPFlags(cmd.Flags()); err != nil {
		return nil, errors.Wrap(err, "failed to bind flags")
	}
	// Define the bindings for the environment variables.
	vi.AutomaticEnv()
	vi.SetEnvPrefix("dynasc")
	vi.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	// Define the bindings for the configuration file.
	if configFile, err := cmd.Flags().GetString("config"); err != nil {
		return nil, errors.Wrap(err, "failed to get a flag named \"config\" correctly")
	} else if configFile != "" {
		slog.Info(fmt.Sprintf("Using configuration file: %s", configFile))
		// Set the configuration file.
		vi.SetConfigFile(configFile)
		// Set "YAML" as configuration type if the file extension is missing.
		if filepath.Ext(configFile) == "" {
			vi.SetConfigType("yaml")
		}
		// Read the configuration.
		if err := vi.ReadInConfig(); err != nil {
			return nil, errors.Wrap(err, "failed to read a configuration file")
		}
	}
	return vi, nil
}

// initRootOption initialize the option for the root command.
func initRootOption(cmd *cobra.Command) (*RootOption, error) {
	// Initialize the configuration.
	vi, err := initConfig(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize the configuration")
	}
	slog.Debug("Configuration has been initialized.", slog.Any("options", vi.AllSettings()))
	// Initialize the option from the configuration.
	opt := &RootOption{
		Triggers: map[string][]string{},
	}
	if err := vi.Unmarshal(opt); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the configuration into an option")
	}
	// Set triggers defined by flags or environment variables after normalization if the triggers is not initialized.
	if vi.ConfigFileUsed() != "" {
		triggers := []*struct {
			Table     string   `mapstructure:"table"`
			Functions []string `mapstructure:"functions"`
		}{}
		if err := vi.UnmarshalKey("triggers", &triggers); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the triggers")
		}
		for _, trigger := range triggers {
			opt.Triggers[trigger.Table] = append(opt.Triggers[trigger.Table], trigger.Functions...)
		}
	} else {
		for _, trigger := range vi.GetStringSlice("triggers") {
			kv := strings.SplitN(trigger, "=", 2)
			if len(kv) != 2 {
				return nil, errors.Newf("failed to parse the trigger: %s: trigger must be in key=value format", kv)
			}
			opt.Triggers[kv[0]] = append(opt.Triggers[kv[0]], kv[1])
		}
	}
	return opt, nil
}

// rootRun executes a main process of the root command.
func rootRun(ctx context.Context, opt *RootOption) error {
	db, err := dynasc.NewDBClient(ctx, opt.DynamoEndpoint)
	if err != nil {
		return errors.Wrap(err, "failed to create a client of DynamoDB")
	}
	dbstreams, err := dynasc.NewDBStreamsClient(ctx, opt.DynamoEndpoint)
	if err != nil {
		return errors.Wrap(err, "failed to create a client of DynamoDB Streams")
	}
	lambda, err := dynasc.NewLambdaClient(ctx, opt.LambdaEndpoint)
	if err != nil {
		return errors.Wrap(err, "failed to create a client of Lambda")
	}
	processor := dynasc.NewLambdaProcessor(lambda)
	// Create a worker
	return dynasc.NewWorker(&dynasc.WorkerConfig{
		DB:        db,
		DBStreams: dbstreams,
		Processor: processor,
	}).Execute(ctx, opt.Triggers)
}
