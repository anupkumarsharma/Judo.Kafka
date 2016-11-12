namespace Judo.Kafka
{
    using System;
    using System.Globalization;
    using System.Linq;

    class DateTimeSurrogate : IAvroSurrogateStrategy
    {
        private static readonly Type[] DateTypes = new[] { 
            typeof(DateTime), 
            typeof(DateTimeOffset) };

        internal const string IsoFormat="yyyy-MM-dd HH:mm:ss.fffzzz";

        public object GetDeserializedObject(object obj, Type targetType)
        {
            if(SurrogateFor(targetType) && obj is string)
            {
                var date = DateTime.ParseExact((string)obj, IsoFormat, CultureInfo.InvariantCulture);
                if(targetType == typeof(DateTimeOffset))
                {
                    return new DateTimeOffset(date);
                }
                else
                {
                    return date;
                }
            }
            
            return obj;
        }

        public object GetObjectToSerialize(object obj, Type targetType)
        {
            if(SurrogateFor(obj.GetType()))
            {
                if(obj is DateTime)
                {
                    return ((DateTime)obj).ToString(IsoFormat);
                }
                else
                {
                    return ((DateTimeOffset)obj).ToString(IsoFormat);
                }

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