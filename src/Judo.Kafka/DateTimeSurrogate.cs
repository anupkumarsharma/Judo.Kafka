namespace Judo.Kafka
{
    using System;
    using System.Linq;

    class DateTimeSurrogate : IAvroSurrogateStrategy
    {

        private const string IsoFormat = "yyyy-MM-dd'T'HH:mm:ssZ";
        private static readonly Type[] DateTypes = new[] { typeof(DateTime), typeof(DateTime?) };

        public object GetDeserializedObject(object obj, Type targetType)
        {
            if(SurrogateFor(targetType))
            {
                var epoch = DateTime.Parse((string)obj);
                return epoch.AddMilliseconds((long)obj);
            }
            
            return obj;
        }

        public object GetObjectToSerialize(object obj, Type targetType)
        {
            if(SurrogateFor(obj.GetType()))
            {
                var originalDate = (DateTime)obj;
                return originalDate.ToString(IsoFormat);
            }
            
            return obj;
        }

        public Type GetSurrogateType(Type type)
        {
            if(SurrogateFor(type))
            {
                return typeof(string);
            }

            return type;
        }

        public bool SurrogateFor(Type type)
        {
            return DateTypes.Contains(type);
        }
    }
}