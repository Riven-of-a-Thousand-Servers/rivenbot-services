package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	"pgcr-processing-service/internal/producer"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitMQ[T any] struct {
	Conn  *amqp091.Connection
	Queue amqp091.Queue
}

type rabbitProducerCloser[T any] struct {
	ch    *amqp091.Channel
	queue string
}

func New[T any](queueName, url string) (*RabbitMQ[T], error) {
	conn, err := amqp091.Dial(url)
	if err != nil {
		slog.Error("Error dialing RabbitMQ", "Error", err)
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		slog.Error("Failed to open a concurrent channel", "Error", err)
		conn.Close()
		return nil, err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		slog.Error("Failed to declare queue", "Error", err)
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &RabbitMQ[T]{
		Conn:  conn,
		Queue: q,
	}, err
}

func (r *RabbitMQ[T]) NewProducer(ctx context.Context) (producer.ProducerCloser[T], error) {
	ch, err := r.Conn.Channel()
	if err != nil {
		slog.Error("Failed to open producer channel", "error", err)
		return nil, err
	}

	return &rabbitProducerCloser[T]{ch: ch, queue: r.Queue.Name}, nil
}

func (i *rabbitProducerCloser[T]) Close() error {
	return i.ch.Close()
}

func (i *rabbitProducerCloser[T]) Produce(ctx context.Context, item T) error {
	bytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	publishing := amqp091.Publishing{
		Headers: map[string]any{
			"source": "Crawler",
		},
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Body:            bytes,
	}

	if err := i.ch.PublishWithContext(ctx, "", i.queue, false, false, publishing); err != nil {
		slog.Error("Unable to publish message", "error", err)
		return err
	}

	return nil
}

// Instantiate a queue
// The name parameter declares the name of the consumer
func (r *RabbitMQ[T]) OpenDeliveryCh(ctx context.Context, consumerName string) (<-chan amqp091.Delivery, *amqp091.Channel, error) {
	ch, err := r.Conn.Channel()
	if err != nil {
		slog.Error("Failed to open amqp channel", "error", err, "consumer", consumerName)
		return nil, nil, err
	}

	delivery, err := ch.ConsumeWithContext(ctx, r.Queue.Name, consumerName, false, false, false, false, nil)
	if err != nil {
		slog.Error("Error declaring consumer for RabbitMQ", "Error", err)
		return nil, nil, err
	}

	return delivery, ch, nil
}

// Instantiate a queue Consumer
// The name parameter declares the name of the consumer
func (r *RabbitMQ[T]) Consumer(ctx context.Context, consumerName string) (<-chan amqp091.Delivery, *amqp091.Channel, error) {
	ch, err := r.Conn.Channel()
	if err != nil {
		slog.Error("Failed to open amqp channel", "error", err, "consumer", consumerName)
		return nil, nil, err
	}

	delivery, err := ch.ConsumeWithContext(ctx, r.Queue.Name, consumerName, false, false, false, false, nil)
	if err != nil {
		slog.Error("Error declaring consumer for RabbitMQ", "Error", err)
		return nil, nil, err
	}

	return delivery, ch, nil
}
