import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.List;

import javax.imageio.ImageIO;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.asprise.util.ocr.OCR;


public class Worker {
	
	private static PrintWriter logger;
	
	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		AWSCredentialsProvider credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
		AmazonSQS sqs = new AmazonSQSClient(credentialsProvider);
		String workersQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/new_image_task_queue";
		String managerQueue = "https://sqs.us-east-1.amazonaws.com/152554501442/done_image_task_queue";
		
		logger = new PrintWriter("/home/ec2-user/worker-logs.log", "UTF-8");
		loggerWrapper("Worker started.");
		
		ReceiveMessageRequest receiveMessageRequest;
		List<Message> messages;
		Message message;
		String image_url = "";
		BufferedImage image = null;
		String parsed_image = "";
		URL url;

		while(true){			
			
			// Receive messages
			receiveMessageRequest = new ReceiveMessageRequest(workersQueue);
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if (messages.size()>0){
				message = messages.get(0);
				image_url = message.getBody();
				try {
					url =new URL(image_url);
					// read the url
					image = ImageIO.read(url);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				// recognizes characters 
				parsed_image= new OCR().recognizeCharacters(image);
				// sends the results.
				sqs.sendMessage(new SendMessageRequest(managerQueue, ""+parsed_image+image_url));
				loggerWrapper("sent - "+parsed_image);
				//delete the message from queue
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(workersQueue, messageRecieptHandle));
			}
			else
			{
				break;
			}
			
		}
		loggerWrapper("out");
//		String ans = ""+parsed_image+image_url;
//		File htmlFile;
//		String[] splited = ans.split("http://");
////		loggerWrapper("1 -"+splited[0]+"\n 2- "+splited[1]);
//		String toAdd = "	<p>\n		<img src=\"http://"+splited[1].replace('\n', ' ')+"\"><br/>\n		"+splited[0]+"\n	</p>";
//		loggerWrapper("toAdd  = "+toAdd);
//		htmlFile = new File("myfile.html");
//		htmlFile.setWritable(true);
//		try {
//			BufferedWriter bw = new BufferedWriter(new FileWriter(htmlFile));
//			bw.write("<html>\n<title>MY OCR</title>\n<body>");
//			bw.write(toAdd);
//			bw.write("</body>\n<html>");
//			bw.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	private static void loggerWrapper(String msg)
	{
		logger.println(msg);
		logger.flush();
	}
}
