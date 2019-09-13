---
page_type: sample
languages:
- csharp
products:
- azure
description: "The Queue Service provides reliable messaging for workflow processing and for communication
between loosely coupled components of cloud services."
urlFragment: storage-queue-dotnet-getting-started
---

# Getting Started with Azure Queue Service in .NET

The Queue Service provides reliable messaging for workflow processing and for communication
between loosely coupled components of cloud services. This sample demonstrates how to perform common tasks including
inserting, peeking, getting and deleting queue messages, as well as creating and deleting queues.

Note: This sample uses the .NET 4.5 asynchronous programming model to demonstrate how to call the Storage Service using the
storage client libraries asynchronous API's. When used in real applications this approach enables you to improve the
responsiveness of your application. Calls to the storage service are prefixed by the await keyword.
If you don't have a Microsoft Azure subscription you can
get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212).

## Running this sample

This sample can be run using either the Azure Storage Emulator that installs as part of this SDK - or by
updating the App.Config file with your AccountName and Key.
To run the sample using the Storage Emulator (default option):

1. Download and Install the Azure Storage Emulator [here](http://azure.microsoft.com/en-us/downloads/).
2. Start the Azure Storage Emulator (once only) by pressing the Start button or the Windows key and searching for it by typing "Azure Storage Emulator". Select it from the list of applications to start it.
3. Set breakpoints and run the project using F10.

To run the sample using the Storage Service

1. Open the app.config file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and uncomment the connection string for the storage service (AccountName=[]...)
2. Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the App.Config file.
3. Set breakpoints and run the project using F10.

## More information
- [What is a Storage Account](http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/)
- [Getting Started with Blobs](http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/)
- [Queue Service Concepts](http://msdn.microsoft.com/en-us/library/dd179353.aspx)
- [Queue Service REST API](http://msdn.microsoft.com/en-us/library/dd179363.aspx)
- [Queue Service C# API](http://go.microsoft.com/fwlink/?LinkID=398944)
- [Delegating Access with Shared Access Signatures](http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/)
- [Storage Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx)
- [Asynchronous Programming with Async and Await](http://msdn.microsoft.com/en-us/library/hh191443.aspx)
