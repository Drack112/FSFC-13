package kafka

import ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type Producer struct {
  ConfigMap *ckafka.ConfigMap
}

func NewKafkaProducer(configMap *ckafka.ConfigMap) *Producer {
  return &Producer{
    ConfigMap: configMap,
  }
}

func (p *Producer) Publish(msg interface{}, key []byte, topic string) error {
  producer, err := ckafka.NewProducer(p.ConfigMap)
  if err != nil {
    return err
  }

  partition := int32(0)

  message := &ckafka.Message{
    TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: partition},
    Key:            key,
    Value:          msg.([]byte),
  }

  err = producer.Produce(message, nil)
  if err != nil {
    return err
  }
  return nil
}
