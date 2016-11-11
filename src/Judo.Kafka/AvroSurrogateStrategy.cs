namespace Judo.Kafka
{
    using System;
    using System.Linq;
    using Microsoft.Hadoop.Avro;

    class AvroSurrogateStrategy : IAvroSurrogate
    {
        private static readonly IAvroSurrogateStrategy[] Strategies = new IAvroSurrogateStrategy[]{new DateTimeSurrogate(), new GuidSurrogate()};
        public object GetDeserializedObject(object obj, Type targetType)
        {
            var surrogate = GetStrategry(targetType);
            if(surrogate != null)
            {
                surrogate.GetDeserializedObject(obj, targetType);
            }

            return obj;
        }

        public object GetObjectToSerialize(object obj, Type targetType)
        {
            var surrogate = GetStrategry(targetType);
            if(surrogate != null)
            {
                surrogate.GetObjectToSerialize(obj, targetType);
            }
            
            return obj;
        }

        public Type GetSurrogateType(Type type)
        {
            var surrogate = GetStrategry(type);
            if(surrogate != null)
            {
                surrogate.GetSurrogateType(type);
            }
            
            return type;
        }

        private IAvroSurrogate GetStrategry(Type targetType)
        {
            return Strategies.FirstOrDefault(s => s.SurrogateFor(targetType));
        }
    }
}