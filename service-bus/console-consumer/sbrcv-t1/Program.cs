using System;

namespace sbrcv_t1
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
    using Azure.Core;
    using Azure.Identity;
    using MathNet.Numerics.Statistics;
    using McMaster.Extensions.CommandLineUtils;
    using Microsoft.Azure.Amqp.Encoding;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Microsoft.Azure.ServiceBus.Primitives;

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
                await RunServiceProcessorAsync();
            }
            else
            {
                await RunReceiveLoopAsync();
            }
        }

        class TokenCredentialTokenProvider : ITokenProvider
        {
            public TokenCredentialTokenProvider(TokenCredential credential)
            {
                Credential = credential;
            }

            public TokenCredential Credential { get; }

            public async Task<SecurityToken> GetTokenAsync(string appliesTo, TimeSpan timeout)
            {
                var trc = new TokenRequestContext(new[] { "https://servicebus.azure.net/.default" });
                var token = await Credential.GetTokenAsync(trc, CancellationToken.None);
                return new JsonSecurityToken(token.Token, appliesTo);
            }
        }

        private async Task RunReceiveLoopAsync()
        {
            var client = new ServiceBusConnection(this.NamespaceName, TransportType.Amqp);
            client.TokenProvider = new TokenCredentialTokenProvider(new DefaultAzureCredential());

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


            var receiver = new MessageReceiver(client, EntityName, this.ReceiveDelete ? ReceiveMode.ReceiveAndDelete : ReceiveMode.PeekLock, RetryPolicy.Default, PrefetchCount);

            Console.CancelKeyPress += async (a, o) => { await receiver.CloseAsync(); };

            Message message = null;
            do
            {
                message = await receiver.ReceiveAsync();
                if (message != null)
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(message.SessionId) ||
                            !string.IsNullOrEmpty(message.Label))
                        {
                            int color = Math.Abs(string.IsNullOrEmpty(message.SessionId) ? message.Label.GetHashCode() : message.SessionId.GetHashCode());
                            Console.BackgroundColor = ConsoleColor.Black;
                            Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                        }
                        //Console.Write("[]");
                        Interlocked.Increment(ref messageCount);
                        if (!this.ReceiveDelete)
                        {
                            receiver.CompleteAsync(message.SystemProperties.LockToken);
                        }
                    }
                    catch
                    {
                        if (!this.ReceiveDelete)
                        {
                            await receiver.AbandonAsync(message.SystemProperties.LockToken);
                        }
                    }
                }
            }
            while (message != null);

            timer.Dispose();
            await receiver.CloseAsync();
            await client.CloseAsync();

        }

        private async Task RunServiceProcessorAsync()
        {
            var client = new ServiceBusConnection(this.NamespaceName, TransportType.Amqp);
            client.TokenProvider = new TokenCredentialTokenProvider(new DefaultAzureCredential());
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


            var receiver = new MessageReceiver(client, EntityName, this.ReceiveDelete ? ReceiveMode.ReceiveAndDelete : ReceiveMode.PeekLock, RetryPolicy.Default, PrefetchCount);
            receiver.RegisterMessageHandler( async (message, ct) =>
            {
                if (!string.IsNullOrEmpty(message.SessionId) ||
                    !string.IsNullOrEmpty(message.Label))
                {
                    int color = Math.Abs(string.IsNullOrEmpty(message.SessionId) ? message.Label.GetHashCode() : message.SessionId.GetHashCode());
                    Console.BackgroundColor = ConsoleColor.Black;
                    Console.ForegroundColor = (ConsoleColor)((color % 14) + 1);
                }
                Console.Write("[]");
                Interlocked.Increment(ref messageCount);
            }, new MessageHandlerOptions(async e => { }) { MaxConcurrentCalls = this.ConcurrentCalls, AutoComplete = true });

            Console.ReadKey();

            timer.Dispose();
            await receiver.CloseAsync();
        }
    }
}