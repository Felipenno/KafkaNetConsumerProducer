using System;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Serilog;
using static System.Console;

namespace kafkaNetConsumer;

internal class Program
{
    static void Main(string[] args)
    {
        var logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();

        const string bootstrapService = "localhost:9092";

        var topics = new List<TopicSpecification>
        {
            new TopicSpecification { Name= "teste-topic-1", NumPartitions=3 },
            new TopicSpecification { Name= "teste-topic-2", NumPartitions=3 },
            new TopicSpecification { Name= "teste-topic-3", NumPartitions=3 }
        };

        string topicSendoLido = topics[0].Name;

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapService,
            GroupId = $"{topicSendoLido}-group-0",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();  
            consumer.Subscribe(topicSendoLido);

            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    logger.Information($"Mensagem lida: {cr.Message.Value}");
                    logger.Information($"Key: {cr.Message.Key}");
                    logger.Information($"Headers: {cr.Message.Headers.Count}");
                    logger.Information($"Topic: {cr.Topic}");
                    logger.Information($"Partition: {cr.Partition}");
                    logger.Information($"Offset: {cr.Offset}");
                    WriteLine();
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
                logger.Warning("Cancelada a execução do Consumer...");
            }
                
        }
        catch (Exception ex)
        {
            logger.Error($"Exceção: {ex.GetType().FullName} | " + $"Mensagem: {ex.Message}");
        }

    }
}