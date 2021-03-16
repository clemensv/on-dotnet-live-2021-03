using System;

namespace sbrcv
{
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
    using Azure.Messaging.ServiceBus;
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

        [Option(ShortName = "s", LongName = "session-mode", Description = "Use session mode")]
        public bool SessionMode { get; }

        [Option(ShortName = "p", LongName = "prefetch-count", Description = "Prefetch count")]
        public int PrefetchCount { get; } = 0;

        [Option(ShortName = "d", LongName = "receive-delete-mode", Description = "Receive/delete mode")]
        public bool ReceiveDelete { get; } = false;

        [Option(ShortName = "c", LongName = "concurrent-calls", Description = "Concurrent calls")]
        public int ConcurrentCalls { get; } = 1;


        private async Task OnExecuteAsync()
        {
            var cred = new DefaultAzureCredential();
            var client = new ServiceBusClient(NamespaceName, cred);
            var messages = new ConcurrentBag<ServiceBusReceivedMessage>();
            var sw = Stopwatch.StartNew();
            var bagStart = sw.ElapsedMilliseconds;

            Console.WriteLine($"Receiving from entity '{EntityName}' in namespace '{NamespaceName}'");

            var timer = new Timer(state =>
            {
                var snapshot = messages;
                var bagDuration = sw.ElapsedMilliseconds - bagStart;
                bagStart = sw.ElapsedMilliseconds;

                messages = new ConcurrentBag<ServiceBusReceivedMessage>();
                Console.ResetColor();
                Console.WriteLine($"\nReceived {snapshot.Count} in {bagDuration} ms");
            }, null, 10000,10000);
            
            var processor = client.CreateProcessor(EntityName,
                new ServiceBusProcessorOptions()
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = this.ConcurrentCalls,
                    ReceiveMode = this.ReceiveDelete? ServiceBusReceiveMode.ReceiveAndDelete:ServiceBusReceiveMode.PeekLock, 
                    PrefetchCount = this.PrefetchCount,
                    MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2)
                });
            processor.ProcessMessageAsync += async args =>
            {
                if (!string.IsNullOrEmpty(args.Message.SessionId) || 
                    !string.IsNullOrEmpty(args.Message.Subject))
                {
                    int color = Math.Abs(string.IsNullOrEmpty(args.Message.SessionId) ? args.Message.Subject.GetHashCode():args.Message.SessionId.GetHashCode());
                    Console.BackgroundColor = ConsoleColor.Black;
                    Console.ForegroundColor = (ConsoleColor)((color % 14)+1);
                }
                Console.Write("[]");
                messages.Add(args.Message);
                await args.CompleteMessageAsync(args.Message);
            };
            processor.ProcessErrorAsync += async args =>
            {

            };
            await processor.StartProcessingAsync();

            Console.ReadKey();

            timer.Dispose();
            await processor.StopProcessingAsync();
        }
    }
}