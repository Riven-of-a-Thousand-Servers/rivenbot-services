package process

type ProcessorConfig struct {
	RedisUrl      string
	RabbitMQUrl   string
	RabbitMQQueue string
	DatasourceUrl string
	Goroutines    int
	Noop          bool
}

func DefaultConfig() ProcessorConfig {
	return ProcessorConfig{
		RedisUrl:      "redis:6379",
		RabbitMQUrl:   "amqp://localhost:5672",
		RabbitMQQueue: "rivenbot",
		DatasourceUrl: "postgresq://postgres:password@postgres:5432/rivenbot",
		Goroutines:    1,
		Noop:          false,
	}
}
