namespace ehsend
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Diagnostics;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Threading.Tasks;
    using Azure.Identity;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Producer;
    using MathNet.Numerics.Statistics;
    using McMaster.Extensions.CommandLineUtils;
    using Microsoft.Azure.Amqp.Encoding;

    class Program
    {
        Random rnd = new Random();

        public static Task<int> Main(string[] args)
            => CommandLineApplication.ExecuteAsync<Program>(args);

        [Required]
        [Option(ShortName = "n", LongName = "namespace-name", Description = "Namespace name")]
        public string NamespaceName { get; }

        [Required]
        [Option(ShortName = "e", LongName = "entity-name", Description = "Entity (queue or topic) name")]
        public string EntityName { get; }

        [Option(ShortName = "c", LongName = "message-count", Description = "Number of messages to send")]
        public int Count { get; } = 1;

        [Option(ShortName = "b", LongName = "batch-size", Description = "Maximum size of message batches (0 = no batching)")]
        public int BatchSize { get; } = 0;

        [Option(ShortName = "s", LongName = "body-size", Description = "Size of message body")]
        public int BodySize { get; } = 1024;

        [Option(ShortName = "w", LongName = "wait-time", Description = "Wait-time (msec) between sends (0 = no wait)")]
        public int Wait { get; } = 0;

        [Option(ShortName = "a", LongName = "async-completion", Description = "Use async completion")]
        public bool AsyncCompletion { get; private set; } = false;

        private async Task OnExecuteAsync()
        {

            Stopwatch sw = Stopwatch.StartNew();
            ConcurrentBag<double> sendDurations = new ConcurrentBag<double>();
            ConcurrentBag<Task> pendingSends = new ConcurrentBag<Task>();
            byte[] payload = new byte[BodySize];
            rnd.NextBytes(payload);

            if (Wait > 0)
            {
                AsyncCompletion = false;
            }

            Console.WriteLine($"Sending {Count} messages to entity '{EntityName}' in namespace '{NamespaceName}'");

            var cred = new DefaultAzureCredential();
            
            var sender = new EventHubProducerClient(NamespaceName, EntityName, cred);

            if (BatchSize == 0)
            {
                if (AsyncCompletion)
                {
                    // first message is sent sync to start the connection
                    long sendStart = sw.ElapsedMilliseconds;
                    var message = CreateEvent(payload);
                    await sender.SendAsync(new [] {message});
                    sendDurations.Add(sw.ElapsedMilliseconds - sendStart);

                    // into the loop!
                    for (int i = 1; i < Count; i++)
                    {
                        sendStart = sw.ElapsedMilliseconds;
                        message = CreateEvent(payload);
                        pendingSends.Add(sender.SendAsync(new[] { message }).ContinueWith(task =>
                        {
                            sendDurations.Add(sw.ElapsedMilliseconds - sendStart);
                        }));
                    }
                    await Task.WhenAll(pendingSends);
                }
                else
                {
                    for (int i = 0; i < Count; i++)
                    {
                        long sendStart = sw.ElapsedMilliseconds;
                        var message = CreateEvent(payload);
                        await sender.SendAsync(new[] { message });
                        sendDurations.Add(sw.ElapsedMilliseconds - sendStart);

                        if (Wait > 0)
                        {
                            await Task.Delay(Wait);
                        }
                    }
                }
            }
            else
            {
                if (AsyncCompletion)
                {
                    var batch = await sender.CreateBatchAsync();
                    for (int i = 0; i < Count; i++)
                    {
                        var message = CreateEvent(payload);

                        if (batch.Count >= BatchSize || !batch.TryAdd(message))
                        {
                            // batch is full, send the batch, make a new one, and add this message there
                            long sendStart = sw.ElapsedMilliseconds;
                            pendingSends.Add(sender.SendAsync(batch).ContinueWith(task =>
                            {
                                sendDurations.Add(sw.ElapsedMilliseconds - sendStart);
                            }));

                            batch = await sender.CreateBatchAsync();
                            batch.TryAdd(message);
                        }

                        if (i + 1 >= Count)
                        {
                            // if the loop is about to end, send the batch
                            long sendStart = sw.ElapsedMilliseconds;
                            pendingSends.Add(sender.SendAsync(batch).ContinueWith(task =>
                            {
                                sendDurations.Add(sw.ElapsedMilliseconds - sendStart);
                            }));
                        }
                    }
                    await Task.WhenAll(pendingSends);
                }
                else
                {
                    var batch = await sender.CreateBatchAsync();
                    for (int i = 0; i < Count; i++)
                    {
                        var message = CreateEvent(payload);
                        if (batch.Count >= BatchSize || !batch.TryAdd(message))
                        {
                            // batch is full, send the batch, make a new one, and add this message there
                            long sendStart = sw.ElapsedMilliseconds;
                            await sender.SendAsync(batch);
                            sendDurations.Add(sw.ElapsedMilliseconds - sendStart);

                            if (Wait > 0)
                            {
                                await Task.Delay(Wait);
                            }

                            batch = await sender.CreateBatchAsync();
                            batch.TryAdd(message);
                        }

                        if (i + 1 >= Count)
                        {
                            // if the loop is about to end, send the batch
                            long sendStart = sw.ElapsedMilliseconds;
                            await sender.SendAsync(batch);
                            sendDurations.Add(sw.ElapsedMilliseconds - sendStart);

                            if (Wait > 0)
                            {
                                await Task.Delay(Wait);
                            }
                        }
                    }
                }
            }

            await sender.CloseAsync();
       
            long elapsed = sw.ElapsedMilliseconds;
            Console.WriteLine($"Time elapsed {elapsed} ms");
            Console.WriteLine($"{sendDurations.Count / (elapsed / 1000.0):F} snd/sec");
            Console.WriteLine($"{Count / (elapsed / 1000.0):F} msg/sec");
            Console.WriteLine($"Min {sendDurations.Min():F} ms");
            Console.WriteLine($"Avg {sendDurations.Average():F} ms");
            Console.WriteLine($"Med {sendDurations.Median():F} ms");
            Console.WriteLine($"StdDev {sendDurations.StandardDeviation():F} ms");
            Console.WriteLine($"50% {sendDurations.Quantile(0.5):F} ms, 95% {sendDurations.Quantile(0.95):F} ms, 99% {sendDurations.Quantile(0.99):F} ms, 99.9 % { sendDurations.Quantile(0.999):F} ms");
        }

        EventData CreateEvent(byte[] payload)
        {
            var message = new EventData(payload);
            return message;
        }
    }
}