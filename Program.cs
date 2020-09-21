using System;

namespace service_bus_test
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Management;

    class Program
    {
        static ISubscriptionClient subscriptionClient;

        public static async Task Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONNECTION_STRING");
            var topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");
            var subscriptionName = Environment.GetEnvironmentVariable("SUBSCRIPTION_NAME");

            await CreateSubscription(connectionString, topicName, subscriptionName);
            subscriptionClient = new SubscriptionClient(connectionString, topicName, subscriptionName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register subscription message handler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            Console.Read();

            await subscriptionClient.CloseAsync();
        }

        static async Task CreateSubscription(string serviceBusConnectionString, string topicName, string subscriptionName)
        {
            var client = new ManagementClient(serviceBusConnectionString);

            var subscriptionDescription = new SubscriptionDescription(topicName, subscriptionName)
            {
                DefaultMessageTimeToLive = new TimeSpan(0, 0, 20),
                AutoDeleteOnIdle = new TimeSpan(0, 5, 0), //subscription will be deleted after 5 min if idle.
                LockDuration = new TimeSpan(0, 0, 20),
                MaxDeliveryCount = 10
            };
            await client.CreateSubscriptionAsync(subscriptionDescription);
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false
            };

            // Register the function that processes messages.
            subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message.
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Complete the message so that it is not received again.
            // This can be done only if the subscriptionClient is created in ReceiveMode.PeekLock mode (which is the default).
            await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the subscriptionClient has already been closed.
            // If subscriptionClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
            // to avoid unnecessary exceptions.
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}
