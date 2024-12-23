using Confluent.Kafka;
using MongoDB.Driver;
using Newtonsoft.Json;
using Orderly.Consumer.Entities;
using Orderly.Consumer.Kafka.Interfaces;

namespace Orderly.Consumer.Kafka.Consumer
{
    public class OrderEventConsumer : IOrderEventConsumer
    {
        private readonly IMongoCollection<Order> _orderCollection;
        private readonly IConsumer<string, string> _consumer;

        public OrderEventConsumer(IMongoCollection<Order> orderCollection, IConsumer<string, string> consumer)
        {
            _orderCollection = orderCollection;
            _consumer = consumer;
        }

        public void StartConsuming(CancellationToken cancellationToken)
        {
            _consumer.Subscribe("orders");

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                var orderEvent = JsonConvert.DeserializeObject<OrderEvent>(consumeResult.Message.Value);

                // Verifica se o pedido já existe no MongoDB
                var existingOrder = _orderCollection.AsQueryable().FirstOrDefault(o => o.Id == orderEvent.Id);

                switch (orderEvent.Operation)
                {
                    case "Create":
                        // Adiciona o novo pedido
                        _orderCollection.InsertOne(orderEvent.Order);
                        Console.WriteLine($"Order Created: {orderEvent.Order.Id}");
                        break;

                    case "Update":
                        // Atualiza o pedido existente
                        _orderCollection.ReplaceOne(o => o.Id == orderEvent.Order.Id, orderEvent.Order);
                        Console.WriteLine($"Order Updated: {orderEvent.Order.Id}");
                        break;

                    case "Delete":
                        // Remove o pedido
                        _orderCollection.DeleteOne(o => o.Id == orderEvent.Order.Id);
                        Console.WriteLine($"Order Deleted: {orderEvent.Order.Id}");
                        break;

                    default:
                        Console.WriteLine("Unknown operation type.");
                        break;
                }

                //if (existingOrder != null)
                //{
                //    // Atualiza o pedido existente
                //    _orderCollection.ReplaceOne(o => o.Id == orderEvent.Id, orderEvent);
                //}
                //else
                //{
                //    // Adiciona o novo pedido
                //    Console.WriteLine($"Order ID: {orderEvent.Id}");
                //    Console.WriteLine($"Order Event: {JsonConvert.SerializeObject(orderEvent)}");
                //    _orderCollection.InsertOne(orderEvent);
                //}
            }
        }
    }
}
