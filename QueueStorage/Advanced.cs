//----------------------------------------------------------------------------------
// Microsoft Azure Storage Team
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;

namespace QueueStorage
{
    public class Advanced
    {

        /// <summary>
        /// Test some of the queue storage operations.
        /// </summary>
        public async Task RunQueueStorageAdvancedOpsAsync()
        {
            try
            {
                //***** Setup *****//
                Console.WriteLine("Getting reference to the storage account.");

                // Retrieve storage account information from connection string
                // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
                CloudStorageAccount storageAccount = Common.CreateStorageAccountFromConnectionString(CloudConfigurationManager.GetSetting("StorageConnectionString"));

                Console.WriteLine("Instantiating queue client.");
                Console.WriteLine(string.Empty);

                // Create a queue client for interacting with the queue service.
                CloudQueueClient cloudQueueClient = storageAccount.CreateCloudQueueClient();

                // List queues
                await ListQueuesSample(cloudQueueClient);

                // Service properties
                await ServicePropertiesSample(cloudQueueClient);

                // CORS Rules
                await CorsSample(cloudQueueClient);

                // Service Stats
                await ServiceStatsSample(cloudQueueClient);

                // Queue Metadata
                await QueueMetadataSample(cloudQueueClient);

                // Queue Acl
                await QueueAclSample(cloudQueueClient);
            }
            catch (Exception ex)
            {
                Console.WriteLine("    Exception thrown. Message = {0}{1}    Strack Trace = {2}", ex.Message, Environment.NewLine, ex.StackTrace);
            }
        }

        /// <summary>
        /// Create, list and delete queues
        /// </summary>
        /// <param name="cloudQueueClient"></param>
        /// <returns></returns>
        private static async Task ListQueuesSample(CloudQueueClient cloudQueueClient)
        {
            // Create 3 queues.

            // Create the queue name -- use a guid in the name so it's unique.
            string baseQueueName = "demotest-" + System.Guid.NewGuid().ToString();

            // Keep a list of the queues so you can compare this list 
            //   against the list of queues that we retrieve.
            List<string> queueNames = new List<string>();

            for (int i = 0; i < 3; i++)
            {
                // Set the name of the queue, then add it to the generic list.
                string queueName = baseQueueName + "-0" + i;
                queueNames.Add(queueName);

                // Create the queue with this name.
                Console.WriteLine("Creating queue with name {0}", queueName);
                CloudQueue cloudQueue = cloudQueueClient.GetQueueReference(queueName);
                try
                {
                    await cloudQueue.CreateIfNotExistsAsync();
                    Console.WriteLine("    Queue created successfully.");
                }
                catch (StorageException exStorage)
                {
                    Common.WriteException(exStorage);
                    Console.WriteLine(
                        "Please make sure your storage account is specified correctly in the app.config - then restart the sample.");
                    Console.WriteLine("Press any key to exit");
                    Console.ReadLine();
                    throw;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("    Exception thrown creating queue.");
                    Common.WriteException(ex);
                    throw;
                }
            }

            Console.WriteLine(string.Empty);
            Console.WriteLine("List of queues in the storage account:");

            // List the queues for this storage account 
            QueueContinuationToken token = null;
            List<CloudQueue> cloudQueueList = new List<CloudQueue>();

            do
            {
                QueueResultSegment segment = await cloudQueueClient.ListQueuesSegmentedAsync(baseQueueName, token);
                token = segment.ContinuationToken;
                cloudQueueList.AddRange(segment.Results);
            }
            while (token != null);

            try
            {
                foreach (CloudQueue cloudQ in cloudQueueList)
                {
                    Console.WriteLine("Cloud Queue name = {0}", cloudQ.Name);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("    Exception thrown listing queues.");
                Common.WriteException(ex);
                throw;
            }

            // Now clean up after yourself, using the list of queues that you created in case there were other queues in the account.
            foreach (string oneQueueName in queueNames)
            {
                CloudQueue cloudQueue = cloudQueueClient.GetQueueReference(oneQueueName);
                cloudQueue.DeleteIfExists();
            }
        }

        /// <summary>
        /// Manage the properties of the Queue service.
        /// </summary>
        /// <param name="queueClient"></param>
        private static async Task ServicePropertiesSample(CloudQueueClient queueClient)
        {
            Console.WriteLine();

            // Get service properties
            Console.WriteLine("Get service properties");
            ServiceProperties originalProperties = await queueClient.GetServicePropertiesAsync();
            try
            {
                // Set service properties
                Console.WriteLine("Set service properties");

                ServiceProperties props = await queueClient.GetServicePropertiesAsync();
                props.Logging.LoggingOperations = LoggingOperations.Read | LoggingOperations.Write;
                props.Logging.RetentionDays = 5;
                props.Logging.Version = Constants.AnalyticsConstants.LoggingVersionV1;

                props.HourMetrics.MetricsLevel = MetricsLevel.Service;
                props.HourMetrics.RetentionDays = 6;
                props.HourMetrics.Version = Constants.AnalyticsConstants.MetricsVersionV1;

                props.MinuteMetrics.MetricsLevel = MetricsLevel.Service;
                props.MinuteMetrics.RetentionDays = 6;
                props.MinuteMetrics.Version = Constants.AnalyticsConstants.MetricsVersionV1;

                await queueClient.SetServicePropertiesAsync(props);
            }
            finally
            {
                // Revert back to original service properties
                Console.WriteLine("Revert back to original service properties");
                await queueClient.SetServicePropertiesAsync(originalProperties);
            }
            Console.WriteLine();
        }

        /// <summary>
        /// Query the Cross-Origin Resource Sharing (CORS) rules for the Queue service
        /// </summary>
        /// <param name="queueClient"></param>
        private static async Task CorsSample(CloudQueueClient queueClient)
        {
            Console.WriteLine();

            // Get service properties
            Console.WriteLine("Get service properties");
            ServiceProperties originalProperties = await queueClient.GetServicePropertiesAsync();
            try
            {
                // Add CORS rule
                Console.WriteLine("Add CORS rule");

                CorsRule corsRule = new CorsRule
                {
                    AllowedHeaders = new List<string> {"*"},
                    AllowedMethods = CorsHttpMethods.Get,
                    AllowedOrigins = new List<string> {"*"},
                    ExposedHeaders = new List<string> {"*"},
                    MaxAgeInSeconds = 3600
                };

                ServiceProperties serviceProperties = await queueClient.GetServicePropertiesAsync();
                serviceProperties.Cors.CorsRules.Add(corsRule);
                await queueClient.SetServicePropertiesAsync(serviceProperties);
            }
            finally
            {
                // Revert back to original service properties
                Console.WriteLine("Revert back to original service properties");
                await queueClient.SetServicePropertiesAsync(originalProperties);
            }
            Console.WriteLine();
        }

        /// <summary>
        /// Retrieve statistics related to replication for the Table service
        /// </summary>
        /// <param name="queueClient"></param>
        private static async Task ServiceStatsSample(CloudQueueClient queueClient)
        {
            Console.WriteLine();

            var originalLocation = queueClient.DefaultRequestOptions.LocationMode;

            Console.WriteLine("Service stats:");
            try
            {
                queueClient.DefaultRequestOptions.LocationMode = LocationMode.SecondaryOnly;
                ServiceStats stats = await queueClient.GetServiceStatsAsync();
                Console.WriteLine("    Last sync time: {0}", stats.GeoReplication.LastSyncTime);
                Console.WriteLine("    Status: {0}", stats.GeoReplication.Status);
            }
            catch (StorageException)
            {
                // only works on RA-GRS (Read Access – Geo Redundant Storage)
            }
            finally
            {
                // Restore original value
                queueClient.DefaultRequestOptions.LocationMode = originalLocation;
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Manage queue metadata
        /// </summary>
        /// <param name="cloudQueueClient"></param>
        /// <returns></returns>
        private static async Task QueueMetadataSample(CloudQueueClient cloudQueueClient)
        {
            // Create the queue name -- use a guid in the name so it's unique.
            string queueName = "demotest-" + Guid.NewGuid();
            CloudQueue queue = cloudQueueClient.GetQueueReference(queueName);

            // Set queue metadata
            Console.WriteLine("Set queue metadata");
            queue.Metadata.Add("key1", "value1");
            queue.Metadata.Add("key2", "value2");

            // Create the queue with this name.
            Console.WriteLine("Creating queue with name {0}", queueName);
            await queue.CreateIfNotExistsAsync();

            // Fetch queue attributes
            // in this case this call is not need but is included for demo purposes
            await queue.FetchAttributesAsync();
            Console.WriteLine("Get queue metadata:");
            foreach (var keyValue in queue.Metadata)
            {
                Console.WriteLine("    {0}: {1}", keyValue.Key, keyValue.Value);
            }

            // Delete queue
            Console.WriteLine("Deleting queue with name {0}", queueName);
            queue.DeleteIfExists();
        }

        /// <summary>
        /// Manage stored access policies specified on the queue
        /// </summary>
        /// <param name="cloudQueueClient"></param>
        /// <returns></returns>
        private static async Task QueueAclSample(CloudQueueClient cloudQueueClient)
        {
            // Create the queue name -- use a guid in the name so it's unique.
            string queueName = "demotest-" + Guid.NewGuid();

            // Create the queue with this name.
            Console.WriteLine("Creating queue with name {0}", queueName);
            CloudQueue queue = cloudQueueClient.GetQueueReference(queueName);
            try
            {
                await queue.CreateIfNotExistsAsync();
                Console.WriteLine("    Queue created successfully.");
            }
            catch (StorageException exStorage)
            {
                Common.WriteException(exStorage);
                Console.WriteLine(
                    "Please make sure your storage account is specified correctly in the app.config - then restart the sample.");
                Console.WriteLine("Press any key to exit");
                Console.ReadLine();
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine("    Exception thrown creating queue.");
                Common.WriteException(ex);
                throw;
            }

            // Set queue permissions
            SharedAccessQueuePolicy accessQueuePolicy = new SharedAccessQueuePolicy();
            accessQueuePolicy.SharedAccessStartTime = new DateTimeOffset(DateTime.Now);
            accessQueuePolicy.SharedAccessExpiryTime = new DateTimeOffset(DateTime.Now.AddMinutes(10));
            accessQueuePolicy.Permissions = SharedAccessQueuePermissions.Update;
            QueuePermissions permissions = new QueuePermissions();
            permissions.SharedAccessPolicies.Add("key1", accessQueuePolicy);
            Console.WriteLine("Set queue permissions");
            await queue.SetPermissionsAsync(permissions);

            // Get queue permissions
            Console.WriteLine("Get queue permissions:");
            permissions = await queue.GetPermissionsAsync();
            foreach (var keyValue in permissions.SharedAccessPolicies)
            {
                Console.WriteLine("  {0}:", keyValue.Key);
                Console.WriteLine("    permissions: {0}:", keyValue.Value.Permissions);
                Console.WriteLine("    start time: {0}:", keyValue.Value.SharedAccessStartTime);
                Console.WriteLine("    expiry time: {0}:", keyValue.Value.SharedAccessExpiryTime);
            }

            // Delete queue
            Console.WriteLine("Deleting queue with name {0}", queueName);
            queue.DeleteIfExists();
        }
    }
}
