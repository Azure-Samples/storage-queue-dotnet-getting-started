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
using System.Threading.Tasks;

namespace QueueStorage
{

    /// <summary>
    /// Azure Queue Service Sample - The Queue Service provides reliable messaging for workflow processing and for communication 
    /// between loosely coupled components of cloud services. This sample demonstrates how to perform common tasks including  
    /// inserting, peeking, getting and deleting queue messages, as well as creating and deleting queues.     
    /// 
    /// Note: This sample uses the .NET 4.5 asynchronous programming model to demonstrate how to call the Storage Service using the 
    /// storage client libraries asynchronous API's. When used in real applications this approach enables you to improve the 
    /// responsiveness of your application. Calls to the storage service are prefixed by the await keyword. 
    /// 
    /// Documentation References: 
    /// - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
    /// - Getting Started with Queues - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-queues/
    /// - Queue Service Concepts - http://msdn.microsoft.com/en-us/library/dd179353.aspx
    /// - Queue Service REST API - http://msdn.microsoft.com/en-us/library/dd179363.aspx
    /// - Queue Service C# API - http://go.microsoft.com/fwlink/?LinkID=398944
    /// - Storage Emulator - http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx
    /// - Asynchronous Programming with Async and Await  - http://msdn.microsoft.com/en-us/library/hh191443.aspx
    /// </summary>
    public class Program
    {
        // *************************************************************************************************************************
        // Instructions: This sample can be run using either the Azure Storage Emulator that installs as part of this SDK - or by
        // updating the App.Config file with your AccountName and Key. 
        // 
        // To run the sample using the Storage Emulator (default option)
        //      1. Start the Azure Storage Emulator (once only) by pressing the Start button or the Windows key and searching for it
        //         by typing "Azure Storage Emulator". Select it from the list of applications to start it.
        //      2. Set breakpoints and run the project using F10. 
        // 
        // To run the sample using the Storage Service
        //      1. Open the app.config file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
        //         uncomment the connection string for the storage service (AccountName=[]...)
        //      2. Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in 
        //         the App.Config file. See http://go.microsoft.com/fwlink/?LinkId=325277 for more information
        //      3. Set breakpoints and run the project using F10. 
        // 
        // *************************************************************************************************************************
        public static void Main(string[] args)
        {
            Console.WriteLine("Azure Storage Queue Sample\n");

            // Create queue, insert message, peek message, read message, change contents of queued message, 
            //    queue 20 messages, get queue length, read 20 messages, delete queue.
            GettingStarted getStarted = new GettingStarted();
            getStarted.RunQueueStorageOperationsAsync().Wait();

            // Get list of queues in storage account.
            Advanced advMethods = new Advanced();
            advMethods.RunQueueStorageAdvancedOpsAsync().Wait();

            Console.WriteLine("Press any key to exit.");
            Console.Read();
        }


    }
}
