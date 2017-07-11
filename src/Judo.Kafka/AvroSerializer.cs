using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Text;

namespace Judo.Kafka
{
    public class AvroSerializer<T> : ISerializer<T>
    {
        private IKafkaSerialzier _kafkaSerializer;
        private string _topic;
        private bool _isKey;

        public AvroSerializer(IKafkaSerialzier kafkaSerializer, string topic, bool isKey)
        {
            _kafkaSerializer = kafkaSerializer;
            _topic = topic;
            _isKey = isKey;
        }
        public byte[] Serialize(T data)
        {
            return _kafkaSerializer.SerializeAsync(data, _isKey, _topic).Result;
        }
    }

    public class AvroDeserializer<T> : IDeserializer<T>
    {
        private IKafkaSerialzier _kafkaSerializer;
        private string _topic;
        private bool _isKey;

        public AvroDeserializer(IKafkaSerialzier kafkaSerializer, string topic, bool isKey)
        {
            _kafkaSerializer = kafkaSerializer;
            _topic = topic;
            _isKey = isKey;
        }

        public T Deserialize(byte[] data)
        {
            return _kafkaSerializer.DeserializeAsync<T>(data, _isKey, _topic).Result;
        }
    }
}
