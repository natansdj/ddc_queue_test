package utils

import (
	"flag"
	"fmt"
	"github.com/assembla/cony"
	"gopkg.in/yaml.v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

type Config struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Vhost        string `yaml:"vhost"`
	Url          string `yaml:"url"`
	ExchangeName string `yaml:"exchangeName"`
	ExchangeType string `yaml:"exchangeType"`
	RoutingKey   string `yaml:"routingKey"`
	QueueName    string `yaml:"queueName"`
}

func ReadConfigFile(cfg *Config) {
	_, file, _, _ := runtime.Caller(0)
	apppath, _ := filepath.Abs(filepath.Dir(filepath.Join(file,
		".."+string(filepath.Separator)+
			".."+string(filepath.Separator))))

	flag.String("config.path", apppath, "")

	f, err := os.Open(apppath + string(filepath.Separator) + "conf/rabbitmq.yml")
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	flag.String("config.host", cfg.Host, "")
	flag.String("config.port", strconv.Itoa(cfg.Port), "")
	flag.String("config.username", cfg.Username, "")
	flag.String("config.vhost", cfg.Vhost, "")
}

func ShowUsageAndStatusConsumer(str ...string) {
	fmt.Printf("%s is running\n\n", str)
	fmt.Println("Configurations :")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
}

func ShowUsageAndStatusProducer(str ...string) {
	fmt.Printf("%s is running\n\n", str)
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Printf("\n\n")

	var body string
	if len(str) >= 2 {
		if body = str[1]; body != "" {
			fmt.Println("Publishing:")
			fmt.Printf("Body: %q\n", body)
			fmt.Printf("\n\n")
		}
	}
}

func PrepFlagsConfig(cfg Config) {
	flag.String("rabbit.url", cfg.Url, "amqp url")
	flag.String("rabbit.queueName", cfg.QueueName, "queueName")
	flag.String("rabbit.routingKey", cfg.RoutingKey, "RoutingKey")
	flag.Parse()
}

func OrderExchange(cfg Config) cony.Exchange {
	return cony.Exchange{
		Name:       cfg.ExchangeName,
		Kind:       cfg.ExchangeType,
		Durable:    true,
		AutoDelete: false,
	}
}

func OrderQueue(cfg Config) *cony.Queue {
	return &cony.Queue{
		Name:       cfg.QueueName,
		Durable:    true,
		AutoDelete: false,
	}
}

func OrderBinding(que *cony.Queue, exc cony.Exchange, cfg Config) cony.Binding {
	return cony.Binding{
		Queue:    que,
		Exchange: exc,
		Key:      cfg.RoutingKey,
	}
}

func TimeLocation() time.Time {
	loc, _ := time.LoadLocation("Asia/Jakarta")
	return time.Now().In(loc)
}
