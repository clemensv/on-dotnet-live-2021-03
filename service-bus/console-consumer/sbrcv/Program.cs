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

        [Option(ShortName = "x", LongName = "processor", Description = "Use processor")]
        public bool UseProcessor { get; }


        private async Task OnExecuteAsync()
        {
            if (UseProcessor)
            {
                if (SessionMode)
                {
                    await RunSessionServiceProcessorAsync();
                }
                else
                {
                    await RunServiceProcessorAsync();
                }

            }
            else
            {
                if ( SessionMode )
                {
                    await RunSessionReceiveLoopAsync();
                }
                else
                {
                    await RunReceiveLoopAsync();
                }
                
            }
        }

        private async Task RunReceiveLoopAsync()
        {
            var cred = new DefaultAzureCredential();
            var client = new ServiceBusClient(NamespaceName, cred);
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


            var receiver = client.CreateReceiver(EntityName, new ServiceBusReceiverOptions()
            {
                PrefetchCount = this.PrefetchCount,
                ReceiveMode = this.ReceiveDelete ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock,
            });

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (a, o) => { cts.Cancel(); };

            do
            {
                var message = await receiver.ReceiveMessageAsync(cancellationToken: cts.Token);
                if (message != null)
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(message.SessionId) ||
                            !string.IsNullOrEmpty(message.Subject))
                        {
                            int color = Math.Abs(string.IsNullOrEmpty(message.SessionId) ? message.Subject.GetHashCode() : message.SessionId.GetHashCode());
                            Console.BackgroundColor = ConsoleColor.Black;
                            Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                        }
                        Console.Write("[]");
                        Interlocked.Increment(ref messageCount);
                        if (!this.ReceiveDelete)
                        {
                            await receiver.CompleteMessageAsync(message, cts.Token);
                        }
                    }
                    catch
                    {
                        if (!this.ReceiveDelete)
                        {
                            await receiver.AbandonMessageAsync(message, cancellationToken: cts.Token);
                        }
                    }
                }
            }
            while (!cts.IsCancellationRequested);

            timer.Dispose();
            await receiver.CloseAsync();
            await client.DisposeAsync();

        }

        private async Task RunSessionReceiveLoopAsync()
        {
            var cred = new DefaultAzureCredential();
            var client = new ServiceBusClient(NamespaceName, cred);
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


            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (a, o) => { cts.Cancel(); };


            do
            {
                var receiver = await client.AcceptNextSessionAsync(EntityName, new ServiceBusSessionReceiverOptions()
                {
                    PrefetchCount = this.PrefetchCount,
                    ReceiveMode = this.ReceiveDelete ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock,
                });

                ServiceBusReceivedMessage message = null;
                do
                {
                    message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), cancellationToken: cts.Token);
                    if (message != null)
                    {
                        try
                        {
                            if (!string.IsNullOrEmpty(message.SessionId) ||
                                !string.IsNullOrEmpty(message.Subject))
                            {
                                int color = Math.Abs(string.IsNullOrEmpty(message.SessionId) ? message.Subject.GetHashCode() : int.Parse(message.SessionId));
                                Console.BackgroundColor = ConsoleColor.Black;
                                Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                            }
                            Console.Write("[]");
                            Interlocked.Increment(ref messageCount);
                            if (!this.ReceiveDelete)
                            {
                                await receiver.CompleteMessageAsync(message, cts.Token);
                            }
                        }
                        catch
                        {
                            if (!this.ReceiveDelete)
                            {
                                await receiver.AbandonMessageAsync(message, cancellationToken: cts.Token);
                            }
                        }
                    }
                }
                while (message != null && !cts.IsCancellationRequested);
                await receiver.CloseAsync();
            }
            while (!cts.IsCancellationRequested);

            timer.Dispose();                             
            await client.DisposeAsync();

        }

        private async Task RunServiceProcessorAsync()
        {
            var cred = new DefaultAzureCredential();
            var client = new ServiceBusClient(NamespaceName, cred);
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

            var options = new ServiceBusProcessorOptions()
            {
                AutoCompleteMessages = true,
                MaxConcurrentCalls = this.ConcurrentCalls,
                ReceiveMode = this.ReceiveDelete ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock,
                PrefetchCount = this.PrefetchCount,
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2)
            };
            var processor = client.CreateProcessor(EntityName, options);
            processor.ProcessMessageAsync += async args =>
            {
                if (!string.IsNullOrEmpty(args.Message.SessionId) ||
                    !string.IsNullOrEmpty(args.Message.Subject))
                {
                    int color = Math.Abs(string.IsNullOrEmpty(args.Message.SessionId) ? args.Message.Subject.GetHashCode() : args.Message.SessionId.GetHashCode());
                    Console.BackgroundColor = ConsoleColor.Black;
                    Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                }
                Console.Write("[]");
                Interlocked.Increment(ref messageCount);
            };
            processor.ProcessErrorAsync += async args =>
            {

            };
            await processor.StartProcessingAsync();

            Console.ReadKey();

            timer.Dispose();
            await processor.StopProcessingAsync();
        }


        private async Task RunSessionServiceProcessorAsync()
        {
            var cred = new DefaultAzureCredential();
            var client = new ServiceBusClient(NamespaceName, cred);
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

            var options = new ServiceBusSessionProcessorOptions()
            {
                AutoCompleteMessages = true,
                MaxConcurrentSessions = 1,
                MaxConcurrentCallsPerSession = this.ConcurrentCalls,
                ReceiveMode = this.ReceiveDelete ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock,
                PrefetchCount = this.PrefetchCount,
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2),
                SessionIdleTimeout = TimeSpan.FromSeconds(1)
            };
            
            var processor = client.CreateSessionProcessor(EntityName, options);
            processor.ProcessMessageAsync += async args =>
            {
                if (!string.IsNullOrEmpty(args.Message.SessionId) ||
                    !string.IsNullOrEmpty(args.Message.Subject))
                {
                    int color = Math.Abs(string.IsNullOrEmpty(args.Message.SessionId) ? args.Message.Subject.GetHashCode() : args.Message.SessionId.GetHashCode());
                    Console.BackgroundColor = ConsoleColor.Black;
                    Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                }
                Console.Write($"[]");
                Interlocked.Increment(ref messageCount);
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