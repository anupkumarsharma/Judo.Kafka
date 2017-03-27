using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Judo.Kafka
{
    public interface ITopicProducer : IDisposable
    {
        Task<Message> ProduceAsync<TMessage>(TMessage payload, int partition = -1);
        Task<Message> ProduceAsync<TKey, TMessage>(TKey key, TMessage message, int partition = -1);
    }
}