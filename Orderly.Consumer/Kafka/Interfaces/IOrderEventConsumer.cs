namespace Orderly.Consumer.Kafka.Interfaces
{
    public interface IOrderEventConsumer
    {
        void StartConsuming(CancellationToken cancellationToken);
    }
}
