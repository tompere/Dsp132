import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.simpleworkflow.flow.annotations.Wait;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * @author Tomp
 *
 */
public class LocalApp {

	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	private static String imagesURLlist = "input_urls_list.txt";
	private static String resultLocationName;
	private static int numberArgs; 
	
	public static void main(String[] args) throws Exception {

		String taskDoneQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/done_task_queue";
		String localAppQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_task_queue";
		
		// Obtain input arguments
		File inputImagesFile;
		
		try
		{
			inputImagesFile = new File(args[0]);
			numberArgs = Integer.parseInt(args[1]);
		}

		catch (Exception e)
		{
			terminateAppWithMessage("Invalid input.", e);
			return;
		}

		// Initialize EC2 service
		init();
		
		// upload the file with the list of images to S3
		String eTag = s3.putObject(new PutObjectRequest("testBucketcm_sdlm4er", imagesURLlist, inputImagesFile)).getETag();
		try
		{	
			sqs.sendMessage(new SendMessageRequest(localAppQueue,imagesURLlist + "::" + numberArgs));
			System.out.println("File uploaded, tag is: " + eTag);
		}
		catch (Exception e)
		{
			terminateAppWithMessage("Input file wasn't uploadeded", e);
			return;
		}

		// Retrieve the manager instance from EC2
		getInstance("Manager");
		
		// help thread that sleeps
		Runnable sleeper = new Runnable(){
		    public void run(){
		        try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					System.out.println("problem sleeping...");
					e.printStackTrace();
				}
		    }
		};
		
		// wait to receive done message from manager via SQS
		String queueMsg = getMessageAndDelete(taskDoneQueue);
		while (queueMsg == "")
		{
			// do nothing and sleep
			queueMsg = getMessageAndDelete(taskDoneQueue);
			sleeper.run();
		}
		
		resultLocationName = queueMsg;
				
		// retrieve result object
		S3Object object = s3.getObject(new GetObjectRequest("testBucketcm_sdlm4er", resultLocationName));
		InputStream objectData = object.getObjectContent();
		int b = objectData.read();
		while ( b > -1 )
		{
			System.out.print((char)b);
			b = objectData.read();
		}
		objectData.close();



	}

	private static void init() throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		ec2 = new AmazonEC2Client(credentialsProvider);
		s3  = new AmazonS3Client(credentialsProvider);
		sqs = new AmazonSQSClient(credentialsProvider);

	}

	private static Instance CreateInstance(String tag)
	{
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		runInstancesRequest.withImageId("ami-76f0061f")
		.withInstanceType("t1.micro")
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName("ass12")
		.withSecurityGroups("ssh")
		.withUserData(createScript("testBucketcm_sdlm4er","manager.jar", "manager"));

		RunInstancesResult runInstancesResult = 
				ec2.runInstances(runInstancesRequest);

		Instance manager = runInstancesResult.getReservation().getInstances().get(0);

		// create manager tag
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(manager.getInstanceId()).withTags(new Tag("Name",tag));
		ec2.createTags(createTagsRequest);

		return manager;
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
		
		// create logs file and allow full access
		ans += "touch /home/ec2-user/manager-logs.log" + "\n";
		ans += "chmod 777 /home/ec2-user/manager-logs.log" + "\n";
		
		// execute jar
		ans += "java -jar /home/ec2-user/" + target + ".jar " + "\n";
		
		System.out.println(ans);

		return new String(Base64.encodeBase64(ans.getBytes()));
	}

	private static Instance getInstance(String tagName)
	{
		List<Reservation> reservations = getInstanceHelper(tagName);
		Instance obtainedInstance = null;

		// retrieve the manager instance from EC2 and return it
		try
		{
			obtainedInstance = reservations.get(0).getInstances().get(0);
			int stateCode = obtainedInstance.getState().getCode();
			// if manager is terminated or in shutting down process
			if (stateCode == 48 || stateCode == 32)
			{
				return CreateInstance(tagName);
			}
			
			// TODO - in order to run jar, manager needs to start over.
			// so, if manager is not being created, it should be rebooted
			
			// if manager is stopped or stopping
			else if (stateCode == 64 || stateCode == 80)
			{
				startIntance(obtainedInstance.getInstanceId());
				return obtainedInstance;
			}
			// if manage is pending or running
			else 
			{
				// block until manager started running
				while (stateCode != 16)
				{
					obtainedInstance = getInstanceHelper(tagName).get(0).getInstances().get(0);
					stateCode = obtainedInstance.getState().getCode();
				}
				return obtainedInstance;	
			}
		}

		// in case no manager was was return null
		catch (Exception e)
		{	
			System.out.println("Creating a new manager.");
			return CreateInstance("Manager");
		}
	}

	private static List<Reservation> getInstanceHelper(String tagName)
	{
		// create a filter
		List<String> valuesT1 = new ArrayList<String>();
		valuesT1.add(tagName);
		Filter filter = new Filter("tag:Name", valuesT1);
		// configure request for instances, based on filter
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
		return result.getReservations();
	}

	private static void startIntance(String instanceID)
	{
		List<String> instancesToStart = new ArrayList<String>();
		instancesToStart.add(instanceID);
		StartInstancesRequest starter = new StartInstancesRequest();
		starter.setInstanceIds(instancesToStart);
		ec2.startInstances(starter);
	}
	
	private static void terminateAppWithMessage(String msg, Exception e)
	{
		System.out.println(msg);
		System.out.println("Error message: " + e.getMessage());
		System.out.println("Program was terminated.");
	}
	
	private static String getMessageAndDelete(String queueUrl) throws IOException{
		String ans = "";
		// Receive messages
		//System.out.println("Receiving messages");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		if (messages.size()>0){
			Message message = messages.get(0);
			ans = message.getBody();
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
		}
		else{
			// System.out.println("queue is empty");
			return "";
		}
		return ans;
	}

}
