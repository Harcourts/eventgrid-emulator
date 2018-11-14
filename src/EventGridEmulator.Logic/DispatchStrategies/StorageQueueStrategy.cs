using EventGridEmulator.Contracts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace EventGridEmulator.Logic.DispatchStrategies
{
    public class StorageQueueStrategy : IDispatcherStrategy
    {
        private readonly CloudStorageAccount _account;
        private readonly ILogger _logger;
        private readonly Dictionary<string, CloudQueue> _queues = new Dictionary<string, CloudQueue>();



        public StorageQueueStrategy(ILogger logger)
        {
            _logger = logger;
            _account = CloudStorageAccount.DevelopmentStorageAccount;
        }

        public async Task DispatchEventAsync(string endpointUrl, EventGridEvent ev)
        {
            _logger.LogInfo(
                $"{Environment.NewLine}Dispatching event (Id: {ev.Id}) to '{endpointUrl}' using '{nameof(StorageQueueStrategy)}'");

            // TODO: Validate url is a suitable queue name

            if (!_queues.TryGetValue(endpointUrl, out var queue))
            {
                _queues[endpointUrl] = queue = GetQueue(_account, endpointUrl);
            }
            
            try
            {
                var json = JsonConvert.SerializeObject(ev);
                var message = new CloudQueueMessage(json);

                await queue.AddMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
        }

        private static CloudQueue GetQueue(CloudStorageAccount account, string queueName)
        {
            var queueClient = account.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(queueName);

            queue.CreateIfNotExists();

            return queue;
        }
    }
}
