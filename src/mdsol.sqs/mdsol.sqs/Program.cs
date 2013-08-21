using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace mdsol.sqs
{
    class Program
    {
        static int messageCount = 0;
        static void Main(string[] args)
        {
            if (args.Length < 2) throw new Exception("'Queue prefix or name' 1");

            var isRead = false;
            if (args[1] == "1") isRead = true;

            var queuePrefix = args[0];

            var sqsClient = new AmazonSQSClient();

            var queuesResult = sqsClient.ListQueues(new ListQueuesRequest{ QueueNamePrefix = queuePrefix });

            if (queuesResult.QueueUrls.Count > 1) throw new Exception("More then one queue found with prefix:" + queuePrefix);
            if (queuesResult.QueueUrls.Count == 0) throw new Exception("No queue found with prefix:" + queuePrefix);

            var queueUrl = queuesResult.QueueUrls.First();

            var dir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sqs-messages", DateTime.Now.ToString("yyyyMMddhhmmss"));
            

            if (isRead)
                Read(sqsClient, dir, queueUrl);
        }

        static void Read(AmazonSQSClient client, string dir, string queueUrl)
        {
            var recieveMessageRequest =
                   new ReceiveMessageRequest();
            var deleteMessageRequest =
                new DeleteMessageRequest();

            recieveMessageRequest.QueueUrl = queueUrl;
            deleteMessageRequest.QueueUrl = queueUrl;

            while (true)
            {
                ReadMessages(client, recieveMessageRequest, deleteMessageRequest, dir);
            }
        }


        static void ReadMessages(AmazonSQSClient client, ReceiveMessageRequest request, DeleteMessageRequest deleteRequest, string dir)
        {
            var result = client.ReceiveMessage(request);
            if (result.Messages.Count != 0)
            {
                if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);

                result.Messages.ForEach(m =>
                {
                    WriteToFile(m, dir);
                    deleteRequest.ReceiptHandle = m.ReceiptHandle;
                    client.DeleteMessage(deleteRequest);
                    Console.WriteLine("Processed: " + m.MessageId);
                }
                );
            }
            else
            {
                Console.WriteLine("No Messages");
            }
            Thread.Sleep(500);
        }

        static void WriteToFile(Message message, string dir)
        {
            System.IO.File.WriteAllText(Path.Combine(dir, message.MessageId), message.Body);
        }
    }
}
