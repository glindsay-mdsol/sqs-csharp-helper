using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace mdsol.sqs
{
    class Program
    {
        static int messageCount = 0;

        private static List<string> studyUuidsForBadStudies = new List<string> { "dcf546eb-d40b-45ba-ae32-6bf2fe15c27a",
"79c08bb3-b495-4daf-bece-68d9394cd253",
"75c2f95c-cf58-4edc-8f47-9a609d85e257",
"afe2e813-a5cf-40e6-8654-b1433c99196f",
"25f18840-5eed-49d1-9919-292652549541",
"0eb1c525-7293-4e0d-ad32-77f671f10892",
"e6e3fbb3-9991-4adb-a326-38cdb6610068",
"26e658ef-7981-425b-a5b4-f34f7172f411",
"3ba6b2c0-8277-11e1-8214-123139318827",
"130d4c71-d577-4074-9957-531ffb1dd073",
"d9a7e8f2-f37c-4620-82d4-891f6ebed3e1",
"a6d2f3d0-d721-4cd2-81d1-604b00494f6e",
"b4a5fcb6-46a9-4a6e-b75d-d51a96e4310c",
"d25c08c2-652e-4f3e-a963-f5cce3bc223c",
"df56167b-d064-4b37-8350-ca4ac88e2fd1",
"542fe08e-6f65-4945-9e27-e57a5b8a140a",
"d118efe7-c5ac-419c-915f-d7ec3eb640ac",
"5b784e08-0f3d-4028-bad4-3348f4c6d4f6",
"5e0f2306-81f9-467c-a8e9-d131d21025f9",
"6a81fa31-44bd-4544-8df1-5a3bdc120985",
"bfd166a2-71f7-11e1-a88b-123139318827",
"bf8adacd-d325-4bd6-87d2-9f3f750ea266",
"375f3ee0-939d-11e0-a41b-12313b067011",
"e9c7199f-d8ef-4fbe-b2c9-6942ac10854a",
"d46edc62-d884-447d-b207-05bb0fd72193",
"19acb349-b9cf-4046-8557-f0d821a9861a",
"56ba514e-eaaa-11e0-87df-12313b023031",
"92358c0d-d81a-42ce-8b10-a716332438e5",
"bb072994-ae26-11e0-9da2-12313d21beb5",
"09fce098-068c-4963-8d99-aaa8eab06a4d",
"809b042e-341f-420a-a2bd-e7bebc7596f7",
"f345f093-6632-4ccf-8d10-eb0809be4f75",
"c5a54785-6b0e-48eb-adfc-66964fbc0e1b",
"fcba5a51-b4d0-42b2-b3fe-8b43aaf544e9" };

        private static BlockingCollection<string> _processedMessageIds = new BlockingCollection<string>();

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Enter Queue prefix or name.");
                return;
            }

            var queuePrefix = args[0];

            AmazonSQSClient sqsClient;

            try
            {
                sqsClient = new AmazonSQSClient();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating SQS client: {0}", ex);
                return;
            }            

            var queuesResult = sqsClient.ListQueues(new ListQueuesRequest { QueueNamePrefix = queuePrefix });

            if (queuesResult.QueueUrls.Count > 1)
            {
                Console.WriteLine("More then one queue found with prefix:" + queuePrefix);
                return;
            }

            if (queuesResult.QueueUrls.Count == 0)
            {
                Console.WriteLine("No queue found with prefix:" + queuePrefix);
                return;
            }

            var queueUrl = queuesResult.QueueUrls.First();

            var dir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "sqs-messages", DateTime.Now.ToString("yyyyMMdd"));

            Console.WriteLine("Base output directory {0}", dir);

            try
            {
                Read(sqsClient, dir, queueUrl);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unhandled exception from Read: {0}", ex);
            }
        }

        private static void Read(AmazonSQSClient client, string dir, string queueUrl)
        {
            var receiveMessageRequest =
                new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    VisibilityTimeout = 30
                };

            var deleteMessageRequest =
                new DeleteMessageRequest
                {
                    QueueUrl = queueUrl
                };

            var queueAttributes =
                client.GetQueueAttributes(new GetQueueAttributesRequest
                {
                    AttributeNames = new List<string> {"All"},
                    QueueUrl = queueUrl
                });
            //TODO: ApproximateNumberOfMessagesDelayed?
            var numberOfMessagesToProcess = queueAttributes.ApproximateNumberOfMessages +
                                            queueAttributes.ApproximateNumberOfMessagesNotVisible;
            //+ queueAttributes.ApproximateNumberOfMessagesDelayed;
            Console.WriteLine("Attempting to process {0} messages.", numberOfMessagesToProcess);

            var parseDir = Path.Combine(dir, "parseMessages");
            var goodDir = Path.Combine(dir, "goodMessages");
            var badDir = Path.Combine(dir, "badMessages");
            var doneDir = Path.Combine(dir, "doneMessages");

            if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
            if (!Directory.Exists(parseDir)) Directory.CreateDirectory(parseDir);
            if (!Directory.Exists(goodDir)) Directory.CreateDirectory(goodDir);
            if (!Directory.Exists(badDir)) Directory.CreateDirectory(badDir);
            if (!Directory.Exists(doneDir)) Directory.CreateDirectory(doneDir);

            #region Download
            //TODO: we need this!
            var numberDownloadedLastIteration = 0;
            var numberRemaining = 0;

            while (_processedMessageIds.Count < numberOfMessagesToProcess)
            {
                numberDownloadedLastIteration = ReadMessages(client, receiveMessageRequest, deleteMessageRequest, dir, parseDir);
            }
            #endregion

            #region Parse
            Directory
                .EnumerateFiles(parseDir)
                .ToList()
                .ForEach(file =>
                {
                    ParseAFile(file, goodDir, badDir);
                    Console.Write("<");
                });

            Console.WriteLine("Done parsing messages.");
            #endregion

            #region Upload
            Console.WriteLine("Started uploading...");

            Directory
                .EnumerateFiles(goodDir)
                .ToList()
                .ForEach(file =>
                {
                    UploadAFile(file, doneDir, queueUrl);
                    Console.Write(">");
                });

            Console.WriteLine("Done uploading messages.");
            #endregion

            Console.WriteLine("Done, exiting.");
        }

        static int ReadMessages(AmazonSQSClient client, ReceiveMessageRequest request, DeleteMessageRequest deleteRequest, string sourceDir, string targetDir)
        {
            var result = client.ReceiveMessage(request);
            var messageCount = result.Messages.Count;

            if (messageCount != 0)
            {
                result.Messages.ForEach(m =>
                {
                    //parse the body
                    var body = m.Body;
                    dynamic messageJson = JObject.Parse(body);

                    //Medidata Message Id
                    var messageId = messageJson.message_id.ToString();

                    //Try to add it to our tracking collection. If we can't, we've already processed it.
                    if (!_processedMessageIds.TryAdd(messageId)) return;

                    var downloadFileName = m.MessageId + ".txt";
                    var downloadPath = Path.Combine(sourceDir, downloadFileName);
                    var parsePath = Path.Combine(targetDir, downloadFileName);

                    File.WriteAllText(downloadPath, m.Body);

                    deleteRequest.ReceiptHandle = m.ReceiptHandle;
                    client.DeleteMessage(deleteRequest);

                    File.Move(downloadPath, parsePath);

                    Console.Write("-");
                });
            }
            else
            {
                Console.WriteLine("No Messages");
            }
            Thread.Sleep(200);
            return messageCount;
        }

        private static void ParseAFile(string sourceFilePath, string goodDirectoryPath, string badDirectoryPath)
        {
            dynamic messageJson = null;

            try
            {
                messageJson = JObject.Parse(File.ReadAllText(sourceFilePath));
            }
            //sometimes the other thread has a lock on the file?
            catch (IOException iex)
            {
                if (iex.Message.IndexOf("use by another process", System.StringComparison.Ordinal) == -1) return;
                throw;
            }

            //Medidata Message Id
            var messageId = messageJson.message_id;

            //func to check for messages that are for bad studies...naughty studies.
            Func<dynamic, bool> checkStudy = (msg) =>
            {
                try
                {
                    var raw = messageJson.data.study;

                    if (raw == null) return false;

                    var studyUuidForMessage = messageJson.data.study.uuid.ToString();

                    return studyUuidsForBadStudies.Contains(studyUuidForMessage);
                }
                catch (Exception ex)
                {
                    return false;
                }
            };

            Func<dynamic, bool> checkDuplicateAssignments = (msg) =>
            {
                try
                {
                    
                
                //TODO: fuck, what if the file is still being downloaded? Probably ignore it and just try the next file.
                //these are the module types that don't allow multiple assignments
                var roleTypesYo = new List<string> { "AP", "AGL", "SGMP", "SGMG" };

                var roles = new List<string>();

                //need linquey goodness
                foreach (var ass in messageJson.data.app_assignments)
                {
                    foreach (var assRole in ass.roles)
                    {
                        roles.Add(assRole.oid.ToString());
                    }
                }

                return roles
                    //get the role oid prefixes
                    .Select(role => role.Split('|')[0])
                    //we only care about roles that are for strict module types
                    .Where(roleTypesYo.Contains)
                    .GroupBy(x => x)
                    //if we have any w/ multiples, go bang
                    .Any(grp => grp.Count() > 1);
                }
                catch (Exception ex)
                {
                    return false;
                }
            };

            var hasBadStudies = checkStudy(messageJson);
            var hasDuplicateAssignments = checkDuplicateAssignments(messageJson);


            Console.WriteLine("Deleting MessageId {0}", messageId);
            if (hasBadStudies) Console.WriteLine("* Is for an illegal study.");
            if (hasDuplicateAssignments) Console.WriteLine("* Has duplicate assignments.");

            var targetDirectoryPath = (hasBadStudies || hasDuplicateAssignments)
                ? badDirectoryPath
                : goodDirectoryPath;

            var targetFilePath = Path.Combine(targetDirectoryPath, Path.GetFileName(sourceFilePath));

            File.Move(sourceFilePath, targetFilePath);
            Console.Write("+");
        }

        static void UploadAFile(string sourceFilePath, string doneDir, string queueUrl)
        {
            //debug...eliminating the idea that we have trouble due to the constructor...remove hard codes
            //var uploadClient = new AmazonSQSClient("AKIAJF2SDIQO2BNKKKUA", "HrM/llOnmUyQek/517+gqS90fHZwnvpqzeLDKAb9",
            //    RegionEndpoint.USEast1);

            string messageBody;

            try
            {
                messageBody = File.ReadAllText(sourceFilePath);
            }
            //sometimes the other thread has a lock on the file?
            catch (IOException iex)
            {
                if (iex.Message.IndexOf("use by another process", System.StringComparison.Ordinal) == -1) return;
                throw;
            }

            var uploadClient = new AmazonSQSClient();

            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = queueUrl,
                MessageBody = messageBody
            };

            var response = uploadClient.SendMessage(sendMessageRequest);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            {
                var exceptionMessage = string.Format("Result from Amazon for message {0} was not ok: {1}", sourceFilePath, response);
                throw new Exception(exceptionMessage);
            }

            var targetFilePath = Path.Combine(doneDir, Path.GetFileName(sourceFilePath));
            File.Move(sourceFilePath, targetFilePath);

            Console.Write("*");
        }
    }
}
