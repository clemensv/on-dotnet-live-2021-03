

namespace ehrcv
{
    using System;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Diagnostics;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Identity;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Processor;
    using Azure.Storage.Blobs;
    using MathNet.Numerics.Statistics;
    using McMaster.Extensions.CommandLineUtils;
    using Microsoft.Azure.Amqp.Encoding;

    class Program
    {
        public static Task<int> Main(string[] args)
            => CommandLineApplication.ExecuteAsync<Program>(args);

        [Required]
        [Option(ShortName = "n", LongName = "namespace-name", Description = "Namespace name")]
        public string NamespaceName { get; }

        [Required]
        [Option(ShortName = "e", LongName = "entity-name", Description = "Entity (queue or topic) name")]
        public string EntityName { get; }

        [Required]
        [Option(ShortName = "g", LongName = "consumer-group", Description = "Consumer group name")]
        public string ConsumerGroupName { get; }

        [Required]
        [Option(ShortName = "s", LongName = "storage-account", Description = "Storage account name for checkpoints")]
        public string StorageAccount { get; }

        [Required]
        [Option(ShortName = "c", LongName = "container-name", Description = "Container in storage account for checkpoints")]
        public string ContainerName { get; }

        [Option(ShortName = "p", LongName = "prefetch-count", Description = "Prefetch count")]
        public int PrefetchCount { get; } = 0;

        [Option(ShortName = "y", LongName = "greedy", Description = "Greedy balancing strategy")]
        public bool Greedy { get; }


        private async Task OnExecuteAsync()
        {
            await RunEventProcessorAsync();
        }

        private async Task RunEventProcessorAsync()
        {
            var cred = new DefaultAzureCredential();

            var storageClient = new BlobContainerClient( new UriBuilder("https", StorageAccount, -1, "test4").Uri, cred);

            var options = new EventProcessorClientOptions()
            {
                PrefetchCount = this.PrefetchCount,
                LoadBalancingStrategy = this.Greedy? LoadBalancingStrategy.Greedy : LoadBalancingStrategy.Balanced,
                LoadBalancingUpdateInterval = TimeSpan.FromSeconds(10)
            };
            var processor = new EventProcessorClient(storageClient, this.ConsumerGroupName, NamespaceName, EntityName, cred, options);
            var partitionEventCount = new ConcurrentDictionary<string, int>();

            long messageCount = 0;
            var sw = Stopwatch.StartNew();
            var bagStart = sw.ElapsedMilliseconds;

            Console.WriteLine($"Receiving from entity '{EntityName}' in namespace '{NamespaceName}'");

            var timer = new Timer(state =>
            {
                var snapshot = Interlocked.Exchange(ref messageCount, 0);
                var bagDuration = sw.ElapsedMilliseconds - bagStart;
                bagStart = sw.ElapsedMilliseconds;

                Console.ResetColor();
                Console.WriteLine($"\nReceived {snapshot / (bagDuration / 1000.0)} msg/sec,  {snapshot} in {bagDuration} ms");
            }, null, 10000, 10000);


            async Task processEventHandler(ProcessEventArgs args)
            {
                try
                {
                    if (args.CancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    string partition = args.Partition.PartitionId;
                    if ( !string.IsNullOrEmpty(args.Partition.PartitionId))
                    {
                        int color = Math.Abs(args.Partition.PartitionId.GetHashCode());
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                    }
                    Console.Write("[]");
                    Interlocked.Increment(ref messageCount);

                    int eventsSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
                        key: partition,
                        addValue: 1,
                        updateValueFactory: (_, currentCount) => currentCount + 1);

                    if (eventsSinceLastCheckpoint >= 50)
                    {
                        await args.UpdateCheckpointAsync();
                        partitionEventCount[partition] = 0;
                    }
                }
                catch
                {
                }
            }

            Task processErrorHandler(ProcessErrorEventArgs args)
            {
                try
                {
                    Debug.WriteLine("Error in the EventProcessorClient");
                    Debug.WriteLine($"\tOperation: { args.Operation }");
                    Debug.WriteLine($"\tException: { args.Exception }");
                    Debug.WriteLine("");
                }
                catch
                {
                }

                return Task.CompletedTask;
            }

            try
            {
                using var cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));

                processor.ProcessEventAsync += processEventHandler;
                processor.ProcessErrorAsync += processErrorHandler;

                try
                {
                    await processor.StartProcessingAsync(cancellationSource.Token);
                    Console.ReadKey();
                }
                catch (TaskCanceledException)
                {
                }
                finally
                {
                    await processor.StopProcessingAsync();
                }
            }
            catch
            {
                
            }
            finally
            {
                processor.ProcessEventAsync -= processEventHandler;
                processor.ProcessErrorAsync -= processErrorHandler;
            }

            await processor.StartProcessingAsync();

            Console.ReadKey();

            timer.Dispose();
            await processor.StopProcessingAsync();
        }
    }
}