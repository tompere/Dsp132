import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * 
 */

/**
 * @author Tal
 *
 */
public class Manager {

	static AmazonSQS sqs;
	static AmazonS3 s3;
	private static AmazonEC2 ec2;
	private static String imagesURLlist;
	private static int numberArgs;
	private static String resultLocationName = "Final.html";
	private static PrintWriter logger;

	public static void main(String[] args) throws Exception {
		
		String localAppQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_task_queue";
		String workersQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_image_task_queue";
		String doneImagesQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/done_image_task_queue";
		String taskDoneQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/done_task_queue";
		boolean queue_is_empty=false;
		int messageCount=0;
		
		logger = new PrintWriter("/home/ec2-user/manager-logs.log", "UTF-8");
		loggerWrapper("Manager started.");
		
		//Initialize s3 and sqs 
		init();
		
		String queueMsg = getMessageAndDelete(localAppQueue);
		while (queueMsg == "")
		{
			loggerWrapper("waiting for image list message from local app...");
			queueMsg = getMessageAndDelete(localAppQueue);
		}
		
		// retrieve image list s3-url from local application
		String[] localAppArgs = queueMsg.split("::");
		imagesURLlist = localAppArgs[0];
		numberArgs = Integer.parseInt(localAppArgs[1]);
		
		loggerWrapper("Got image list location (" + imagesURLlist + ") and num (" + numberArgs + ")");

		//get image list
		S3Object imageList = s3.getObject(new GetObjectRequest("testBucketcm_sdlm4er",imagesURLlist));

		//read images file
		InputStream objectData = imageList.getObjectContent();
		int c = objectData.read();
		String imageUrl = "";
		while ( c > -1 )
		{
			imageUrl += (char)c;
			if ((char)c == '\n')
			{
				try {
					// Send a message
					loggerWrapper("Got " + imageUrl);
					sqs.sendMessage(new SendMessageRequest(workersQueue, imageUrl));
					loggerWrapper("Sent " + imageUrl + " to queue (" + workersQueue + ")");
					messageCount++;
					imageUrl="";
				} catch (Exception e) {
					loggerWrapper("An error occurred during message sending");
				}
			}
			c = objectData.read();
		}
		objectData.close();

		loggerWrapper("Sent " + messageCount + " tasks to queue.");
		
		CreateInstance((int)Math.ceil((messageCount / (double)numberArgs)));

		Set<String> attrs = new HashSet<String>();
		attrs.add("ApproximateNumberOfMessages");
		attrs.add("ApproximateNumberOfMessagesNotVisible");
		GetQueueAttributesRequest attrRequest = new GetQueueAttributesRequest(workersQueue).withAttributeNames(attrs);
		Map<String,String> result;
		int numOfVisible,numOfNotVisible=-1;

		while(!queue_is_empty){
			result = sqs.getQueueAttributes(attrRequest).getAttributes();
			numOfVisible = Integer.parseInt(result.get("ApproximateNumberOfMessages"));
			numOfNotVisible = Integer.parseInt(result.get("ApproximateNumberOfMessagesNotVisible"));
			if ((numOfVisible + numOfNotVisible) == 0){
				queue_is_empty = true;
			}
			loggerWrapper("there are - " +(numOfVisible + numOfNotVisible)+" messages in the workers's queue");
			loggerWrapper("when "+numOfVisible+" are visible, and"+numOfNotVisible+" are NOT");
			// queue_is_empty = true;
		}

		loggerWrapper("all done! creating html file");
		queue_is_empty = false;

		File htmlFile;
		String[] splitedAns;
		String returndAns ="";
		htmlFile = new File("myfile.html");
		BufferedWriter bw = new BufferedWriter(new FileWriter(htmlFile));
		bw.write("<html>\n<title>MY OCR</title>\n<body>");
		htmlFile.setWritable(true);
		attrRequest = new GetQueueAttributesRequest(doneImagesQueue).withAttributeNames(attrs);

		while(!queue_is_empty){
			result = sqs.getQueueAttributes(attrRequest).getAttributes();
			numOfVisible = Integer.parseInt(result.get("ApproximateNumberOfMessages"));
			numOfNotVisible = Integer.parseInt(result.get("ApproximateNumberOfMessagesNotVisible"));
			if ((numOfVisible + numOfNotVisible) == 0){
				queue_is_empty = true;
			}
			else
			{
				returndAns = getMessageAndDelete(doneImagesQueue);
				if (returndAns != ""){
					returndAns = returndAns.substring(returndAns.lastIndexOf("('*')." + 1));
					splitedAns = returndAns.split("http://");
					String toAdd = "<p>\n<img src=\"http://" + splitedAns[1].replace('\n', ' ')+"\"><br/>\n"+splitedAns[0]+"\n</p>\n\n";
					bw.write(toAdd);
				}
			}
		}

		bw.write("</body>\n</html>");
		bw.close();

		// upload the file with the list of images to S3
		String eTag = s3.putObject(new PutObjectRequest("testBucketcm_sdlm4er",resultLocationName, htmlFile)).getETag();
		if (eTag != null)
		{
			loggerWrapper("File uploaded, tag is: " + eTag);
		}

		//send "done" message
		sqs.sendMessage(new SendMessageRequest(taskDoneQueue,resultLocationName));
		
		loggerWrapper("Done message with location (" + resultLocationName + ") was sent to queue");
		
		logger.close();
	}
	
	private static void init() throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		sqs = new AmazonSQSClient(credentialsProvider);
		s3  = new AmazonS3Client(credentialsProvider);
		ec2 = new AmazonEC2Client(credentialsProvider);
	}

	private static String getMessageAndDelete(String queueUrl) throws IOException{
		String ans = "";
		// Receive messages
		//loggerWrapper("Receiving messages");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		if (messages.size()>0){
			Message message = messages.get(0);
			ans = message.getBody();
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
		}
		else{
			//loggerWrapper("queue is empty");
			return "";
		}
		return ans;
	}
	
	private static List<Instance> CreateInstance(int numOfInstances)
	{
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		runInstancesRequest.withImageId("ami-76f0061f")
		.withInstanceType("t1.micro")
		.withMinCount(numOfInstances)
		.withMaxCount(numOfInstances)
		.withKeyName("ass12")
		.withSecurityGroups("ssh")
		.withUserData(createScript("testBucketcm_sdlm4er","worker.jar", "worker"));

		RunInstancesResult runInstancesResult = 
				ec2.runInstances(runInstancesRequest);

		List<Instance> instances = runInstancesResult.getReservation().getInstances();
		ArrayList<String> instancesIDs = new ArrayList<String>();
		for (Instance inst : instances)
		{
			instancesIDs.add(inst.getInstanceId());
		}
		// create manager tag
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		//createTagsRequest.withResources(manager.getInstanceId()).withTags();
		createTagsRequest.withResources(instancesIDs).withTags(new Tag("Name","Worker"));
		ec2.createTags(createTagsRequest);

		return instances;
	}

	private static String createScript(String bucket, String key, String target)
	{
		String ans = "#!/bin/bash" + "\n";
		ans += "sudo sh" + "\n";
		// install s3curl
		ans += "mkdir -p /tmp/aws" + "\n";
		ans += "mkdir -p /usr/local/share" + "\n"; 
		ans += "mkdir -p /usr/local/bin" + "\n";
		ans += "curl --silent --output /tmp/aws/s3-curl.zip http://s3.amazonaws.com/doc/s3-example-code/s3-curl.zip" + "\n";
		ans += "unzip -d /tmp/aws /tmp/aws/s3-curl.zip" + "\n";
		ans += "chmod 755 /tmp/aws/s3-curl/s3curl.pl" + "\n";
		ans += "rm -fR /usr/local/share/s3-curl" + "\n";
		ans += "mv /tmp/aws/s3-curl /usr/local/share" + "\n";
		ans += "mv /usr/local/share/s3-curl/s3curl.pl /usr/local/bin" + "\n";
		ans += "rm -fR /tmp/aws/s3-curl*" + "\n";
		// install perl support library for s3-curl
		ans += "yum install -y -q perl-Digest-HMAC" + "\n";
		// secure credentials for s3curl
		ans += "OUTPT=\"%awsSecretAccessKeys = " +
				"(main => " +
				"{id => 'AKIAICNFQQXI3GJCZIEQ'," +
				"key => 'vyr6cS4B/YdCDL03Hjc+qpfvJOJUOFG9EhgJhV00',},);\"" + "\n";		
		ans += "echo $OUTPT > \"/usr/local/bin/.s3curl\"" + "\n";
		ans += "chmod 600 /usr/local/bin/.s3curl" + "\n";
		// retrieve jar from s3 via s3curl client
		ans += "/usr/local/bin/s3curl.pl " +
				"--id main " +
				"-- https://s3.amazonaws.com/" + bucket + "/" + key +
				" > /home/ec2-user/" + target + ".jar" + "\n";
		
		// get Asprise OCR library & set Asprise OCR in library path
		ans += "wget -q -P /home/ec2-user/ http://download.asprise.net/software/ocr-v4/Asprise-OCR-Java-Linux_x86_32bit-4.0.zip " + "\n";
		ans += "unzip -d /home/ec2-user/Asprise-OCR /home/ec2-user/Asprise-OCR-Java-Linux_x86_32bit-4.0.zip" + "\n";
		ans += "yum install -y -q libstdc++.so.5 " + "\n";
		ans += "export LD_LIBRARY_PATH=\"/home/ec2-user/Asprise-OCR\":$LD_LIBRARY_PATH" + "\n";
		
		ans += "chmod 777 /home/ec2-user/worker-logs.log" + "\n";
		
		// execute jar
		ans += "java -jar /home/ec2-user/" + target + ".jar " + "\n";
		
		loggerWrapper("Worker script is " + "\n" + ans + "===========================");
		
		return new String(Base64.encodeBase64(ans.getBytes()));
	}
	
	private static void loggerWrapper(String msg)
	{
		logger.println(msg);
		logger.flush();
	}

}

