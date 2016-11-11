using System;
using System.Threading.Tasks;
using RdKafka;

namespace Judo.Kafka
{
    public interface ITopicProducer : IDisposable
    {
        Task<DeliveryReport> ProduceAsync<TMessage>(TMessage payload, int partition = -1);
        Task<DeliveryReport> ProduceAsync<TKey, TMessage>(TKey key, TMessage message, int partition = -1);
    }
}