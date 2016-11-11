namespace Judo.Kafka
{
    using System;
    using Microsoft.Hadoop.Avro;

    interface IAvroSurrogateStrategy : IAvroSurrogate
    {
        bool SurrogateFor(Type type);
    }
}