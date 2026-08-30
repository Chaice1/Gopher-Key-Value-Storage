package config

import (
	"log"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	App         string `yaml:"app" env:"APP_NAME"`
	NodeID      string `yaml:"node_id" env:"NODE_ID"`
	HTTPPort    int    `yaml:"http_port" env:"HTTP_PORT"`
	GRPCPort    int    `yaml:"grpc_port" env:"GRPC_PORT"`
	NodeAddress string `yaml:"node_address" env:"NODE_ADDRESS"`
	Peers       string `yaml:"peers" env:"PEERS"`
}

type Peer struct {
	ID   string
	Addr string
}

func MustLoad() *Config {

	configPath := os.Getenv("CONFIG_PATH")
	var cfg Config

	if configPath != "" {
		if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
			log.Fatalf("failed to read config: %s", err)
		}
	} else {
		if err := cleanenv.ReadEnv(&cfg); err != nil {
			log.Fatalf("failed to read env: %s", err)
		}
	}
	return &cfg
}
