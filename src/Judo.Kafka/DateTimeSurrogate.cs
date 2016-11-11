namespace Judo.Kafka
{
    using System;
    using System.Linq;

    class DateTimeSurrogate : IAvroSurrogateStrategy
    {
        private static readonly Type[] DateTypes = new[] { typeof(DateTime), typeof(DateTime?) };

        public object GetDeserializedObject(object obj, Type targetType)
        {
            if(SurrogateFor(targetType))
            {
                var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                return epoch.AddMilliseconds((long)obj);
            }
            
            return obj;
        }

        public object GetObjectToSerialize(object obj, Type targetType)
        {
            if(SurrogateFor(obj.GetType()))
            {
                var originalDate = (DateTime)obj;
                var t = originalDate - new DateTime(1970, 1, 1);
                return (t.TotalSeconds * 1000);
            }
            
            return obj;
        }

        public Type GetSurrogateType(Type type)
        {
            if(SurrogateFor(type))
            {
                return typeof(long);
            }

            return type;
        }

        public bool SurrogateFor(Type type)
        {
            return DateTypes.Contains(type);
        }
    }
}