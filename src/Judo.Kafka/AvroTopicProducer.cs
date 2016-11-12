using System.Threading.Tasks;
using RdKafka;

namespace Judo.Kafka
{
    class AvroTopicProducer : ITopicProducer
    {
        private readonly Producer _producer;
        
        private readonly Topic _topic;

        private readonly IKafkaSerialzier _kafkaSerializer;

        public AvroTopicProducer(Producer producer, Topic topic, IKafkaSerialzier kafkaSerializer)
        {
            _producer = producer;
            _topic = topic;
            _kafkaSerializer = kafkaSerializer;
        }

        public Task<DeliveryReport> ProduceAsync<TMessage>(TMessage payload, int partition = -1)
        {
            var serializeTask =  _kafkaSerializer.SerializeAsync(payload, false, _topic.Name);
            Task.WaitAll(serializeTask);
            return _topic.Produce(serializeTask.Result, partition: partition);
        }

        public async Task<DeliveryReport> ProduceAsync<TKey, TMessage>(TKey key, TMessage message, int partition = -1)
        {
            var keyPayloadTask = _kafkaSerializer.SerializeAsync(key, true, _topic.Name);
            var valuePayloadTask = _kafkaSerializer.SerializeAsync(message, false, _topic.Name);
            Task.WaitAll(keyPayloadTask, valuePayloadTask);
            return await _topic.Produce(valuePayloadTask.Result, keyPayloadTask.Result, partition: partition);
        }

        public void Dispose()
        {
            _topic.Dispose();
            _producer.Dispose();
        }
    }
}