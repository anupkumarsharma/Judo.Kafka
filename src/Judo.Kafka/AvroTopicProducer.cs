using System.Threading.Tasks;
using RdKafka;

namespace Judo.Kafka
{
    class AvroTopicProducer : ITopicProducer
    {
        private readonly Topic _topic;
        private readonly Producer _producer;
        private readonly IKafkaSerialzier _kafkaSerializer;

        public AvroTopicProducer(Producer producer, Topic topic, IKafkaSerialzier kafkaSerializer)
        {
            _producer = producer;
            _topic = topic;
            _kafkaSerializer = kafkaSerializer;
        }

        public async Task<DeliveryReport> ProduceAsync<TMessage>(TMessage payload, int partition = -1)
        {
            var serializeTask =  await _kafkaSerializer.SerializeAsync(payload, false, _topic.Name);
            var deliveryReport = await _topic.Produce(serializeTask, partition: partition);
            return deliveryReport;
        }

        public async Task<DeliveryReport> ProduceAsync<TKey, TMessage>(TKey key, TMessage message, int partition = -1)
        {
            var keyPayloadTask = _kafkaSerializer.SerializeAsync(key, true, _topic.Name);
            var valuePayloadTask = _kafkaSerializer.SerializeAsync(message, false, _topic.Name);
            var deliveryReport = await _topic.Produce(await valuePayloadTask, await keyPayloadTask, partition: partition);
            return deliveryReport;
        }

        public void Dispose()
        {
            _topic.Dispose();
            _producer.Dispose();
        }
    }
}