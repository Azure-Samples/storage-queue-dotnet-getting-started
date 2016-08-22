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
using System.Threading.Tasks;

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
                        Console.WriteLine("Please make sure your storage account is specified correctly in the app.config - then restart the sample.");
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
                IEnumerable<CloudQueue> cloudQueueList = cloudQueueClient.ListQueues();
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
            catch (Exception ex)
            {
                Console.WriteLine("    Exception thrown. Message = {0}{1}    Strack Trace = {2}", ex.Message, Environment.NewLine, ex.StackTrace);
            }

        }
    }
}
