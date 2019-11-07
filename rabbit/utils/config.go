package utils

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"path/filepath"
	"runtime"
)

type Config struct {
	Runmode  string `yaml:"runmode"`
	RabbitMQ struct {
		Url          string `yaml:"url"`
		ExchangeName string `yaml:"exchangeName"`
		ExchangeType string `yaml:"exchangeType"`
		RoutingKey   string `yaml:"routingKey"`
		QueueName    string `yaml:"queueName"`
	} `yaml:"rabbitmq"`
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}

func ReadConfigFile(cfg *Config) {
	_, file, _, _ := runtime.Caller(0)
	apppath, _ := filepath.Abs(filepath.Dir(filepath.Join(file, ".."+string(filepath.Separator))))

	f, err := os.Open(apppath + string(filepath.Separator) + "conf/config.yml")
	if err != nil {
		processError(err)
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		processError(err)
	}
}
