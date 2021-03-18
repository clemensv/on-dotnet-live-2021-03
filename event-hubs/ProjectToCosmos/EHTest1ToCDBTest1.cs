using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace ProjectToCosmos
{
    public class Functions
    {
        [FunctionName("EHTest1ToCDBTest1")]
        [ExponentialBackoffRetry(-1, "00:00:05", "00:05:00")]
        public static async Task EHTest1ToCDBTest1(
        [EventHubTrigger("test1", ConsumerGroup = "EHTest1ToCDBTest1", Connection = "EHTest1ToCDBTest1-source-connection")] EventData[] input,
        [CosmosDB(databaseName: "test1", collectionName: "test1", ConnectionStringSetting = "CDBTest1-connection")] IAsyncCollector<object> output,
        ILogger log)
        {
            foreach (var ev in input)
            {
                var id = ev.SystemProperties.PartitionKey ?? "0";                    
                var record = new
                {
                    id = ev.SystemProperties.PartitionKey,
                    data = Convert.ToBase64String(ev.Body),
                    properties = ev.Properties
                };
                await output.AddAsync(record);
            }
        }
    }
}
