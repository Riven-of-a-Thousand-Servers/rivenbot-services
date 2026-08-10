package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	"pgcr-processing-service/internal/consumer"
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
func (r *RabbitMQ[T]) Consume(ctx context.Context) (<-chan consumer.Delivery[T], error) {
	ch, err := r.Conn.Channel()
	if err != nil {
		slog.Error("Failed to open amqp channel for consumer", "error", err)
		return nil, err
	}

	deliveries, err := ch.ConsumeWithContext(ctx, r.Queue.Name, "pgcr-consumer", false, false, false, false, nil)
	if err != nil {
		slog.Error("Error declaring consumer for RabbitMQ", "Error", err)
		return nil, err
	}

	out := make(chan consumer.Delivery[T])
	go func() {
		defer close(out)
		defer ch.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}

				var item T
				if err := json.Unmarshal(d.Body, &item); err != nil {
					slog.Error("Failed to unmarshal delivery, nacking", "error", err)
					d.Nack(false, false)
					continue
				}

				delivery := consumer.Delivery[T]{
					Payload: item,
					Headers: d.Headers,
					Ack:     func() error { return d.Ack(false) },
					Nack:    func(requeue bool) error { return d.Nack(false, requeue) },
				}

				select {
				case out <- delivery:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}
