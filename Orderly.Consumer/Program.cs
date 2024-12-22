using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using Orderly.Consumer.Entities;
using Orderly.Consumer.Kafka.Consumer;
using Orderly.Consumer.Kafka.Interfaces;

class Program
{
    static void Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        BsonSerializer.RegisterSerializer(new GuidSerializer(MongoDB.Bson.GuidRepresentation.Standard));

        var serviceProvider = new ServiceCollection()
            .AddSingleton(sp =>
            {
                var connectionString = configuration["MongoDbSettings:ConnectionString"];
                var databaseName = configuration["MongoDbSettings:DatabaseName"];

                var mongoClient = new MongoClient(connectionString);
                return mongoClient.GetDatabase(databaseName).GetCollection<Order>("Orders");
            })
            .AddSingleton(sp =>
            {
                return new ConsumerConfig
                {
                    BootstrapServers = configuration["Kafka:BootstrapServers"],
                    GroupId = configuration["Kafka:GroupId"],
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };
            })
            .AddSingleton<IConsumer<string, string>>(sp =>
            {
                var config = sp.GetRequiredService<ConsumerConfig>();
                return new ConsumerBuilder<string, string>(config).Build();
            })
            .AddSingleton<IOrderEventConsumer, OrderEventConsumer>()
            .BuildServiceProvider();

        var consumer = serviceProvider.GetService<IOrderEventConsumer>();
        consumer?.StartConsuming(CancellationToken.None);
    }
}
