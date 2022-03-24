package github.com/TagginRaggin/RedisQueue

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type ProducerClient struct {
	rdb   redis.Client
	queue string
}

type ConsumerClient struct {
	rdb   redis.Client
	queue string
}

func NewProducerClient(address string, pass string, q string, db int) ProducerClient {
	var producer ProducerClient
	producer.rdb = *redis.NewClient(&redis.Options{
		Addr:     address,
		Password: pass,
		DB:       db,
	})
	producer.queue = q
	return producer
}

func (p *ProducerClient) Enqueue(message interface{}) (bool, error) {
	/*
		jformat, err := json.Marshal(message)
		if err != nil {
			return false, err
		}
	*/

	erro := p.rdb.RPush(ctx, p.queue, message).Err()
	if erro != nil {
		return false, erro
	}
	return true, nil

}

func NewConsumerClient(address string, pass string, q string, db int) *ConsumerClient {
	var consumer ConsumerClient
	consumer.rdb = *redis.NewClient(&redis.Options{
		Addr:     address,
		Password: pass,
		DB:       db,
	})
	consumer.queue = q
	return &consumer
}
func (c *ConsumerClient) Dequeue() (interface{}, error) {
	message, err := c.rdb.BLPop(ctx, time.Second, c.queue).Result()
	if err != nil {
		return nil, err
	}
	//message[0] is the key we put it on the queue with.
	return message[1], nil

}
