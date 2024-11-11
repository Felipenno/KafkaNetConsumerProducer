
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using static System.Console;

const string bootstrapService = "localhost:9092";

var topics = new List<TopicSpecification> 
{
    new TopicSpecification { Name= "teste-topic-1", NumPartitions=3 },
    new TopicSpecification { Name= "teste-topic-2", NumPartitions=3 },
    new TopicSpecification { Name= "teste-topic-3", NumPartitions=3 }
};



try
{
    using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapService }).Build();
    var dadosKafka = adminClient.GetMetadata(TimeSpan.FromSeconds(15));

    WriteLine("Tópicos Existentes (Exceto os padrões): ");
    var topicosExistentes = dadosKafka.Topics.Where(x => topics.Select(x => x.Name).Contains(x.Topic)).ToList();
    topicosExistentes.ForEach(x => WriteLine($"> Nome: {x.Topic} | Partições:{x.Partitions.Count}"));
    WriteLine();


    var descTopics = new DescribeTopicsResult();

    try
    {
        WriteLine("Tópicos Existentes Detalhes (Exceto os padrões): ");
        var topicCollection = TopicCollection.OfTopicNames(topicosExistentes.Select(x => x.Topic));
        descTopics = await adminClient.DescribeTopicsAsync(topicCollection);
        descTopics?.TopicDescriptions?.ForEach(x => WriteLine($"> Id:{x.TopicId} Nome:{x.Name} Patições:{x.Partitions.Count}"));
        WriteLine();
    }
    catch (DescribeTopicsException ex)
    {
        WriteLine($"Erro ao obter detalhe dos tópicos: {ex.Message}");
    }

    try
    {
        var topicosNaoCriados = topicosExistentes.Where(topic => !(descTopics?.TopicDescriptions?.Select(x => x.Name)?.Contains(topic.Topic)).GetValueOrDefault());
        if (topicosNaoCriados != null && topicosNaoCriados.Any())
        {
            WriteLine("Criando tópicos...");
            await adminClient.CreateTopicsAsync(topics);
            WriteLine($"> Tópicos Criados com sucesso: {string.Join(',', topicosNaoCriados)}");
            WriteLine();
        }
    }
    catch (DescribeTopicsException ex)
    {
        WriteLine($"Erro ao criar os tópicos: {ex.Message}");
    }


    //await adminClient.CreatePartitionsAsync(new List<PartitionsSpecification> { new PartitionsSpecification { Topic = topicName, IncreaseTo = newPartitionsCount } });

    var config = new ProducerConfig
    {
        BootstrapServers = bootstrapService,
        Acks = Acks.All
    };

    using var producer = new ProducerBuilder<string, string>(config).Build();

    var continuar = true;
    while (continuar)
    {
        Write("Inserir mensagem: ");
        var input = ReadLine();

        var msgInput = string.IsNullOrEmpty(input) ? "Sem mensagem" : input;
        var mensagem = new Message<string, string> { Key = "testekey", Value = msgInput };
        var topicPartition = new TopicPartition("teste-topic-1", new Partition(1));

        var result = await producer.ProduceAsync(topicPartition, mensagem);

        WriteLine("result:");
        WriteLine(result.Status.ToString());
        WriteLine();

        Write("Continuar? (S=Sim N=Não) ");
        var inputSair = ReadLine();
        continuar = !string.IsNullOrEmpty(inputSair) && inputSair.Equals("S", StringComparison.OrdinalIgnoreCase);
        Clear();
    }

    
}
catch (Exception ex)
{
    WriteLine("erro");
    WriteLine(ex);
}

