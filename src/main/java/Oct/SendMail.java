package Oct;

import java.util.Properties;
import java.util.UUID;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.sun.mail.util.MailSSLSocketFactory;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/11/14 15:45
 */
public class SendMail {
	public SendMail(String email) throws Exception {
		String code = UUID.randomUUID().toString().replaceAll("-", "");

		//获取系统属性
		Properties prop = System.getProperties();
		prop.setProperty("mail.transport.protocol", "smtp");
		prop.setProperty("mail.smtp.host", "smtp.exmail.qq.com");
		prop.setProperty("mail.smtp.port", "465");
		prop.setProperty("mail.smtp.auth", "true");
		prop.put("mail.smtp.ssl.enable", "true");

		MailSSLSocketFactory sf;
		sf = new MailSSLSocketFactory();
		sf.setTrustAllHosts(true);
		prop.put("mail.smtp.ssl.socketFactory", sf);

		Session session = Session.getDefaultInstance(prop, new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("service.bigdata@gimc.cn", "mSBJmxySiUfnywJs");
			}
		});
		session.setDebug(true);

		// 2.创建邮件对象
		Message message = new MimeMessage(session);
		// 2.1设置发件人
		message.setFrom(new InternetAddress("service.bigdata@gimc.cn"));
		// 2.2设置接收人
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(email));
		// 2.3设置邮件主题
		message.setSubject("【GIMC云】欢迎使用GIMC云，请完成邮箱验证");

		// 2.4设置邮件内容
		StringBuffer content = new StringBuffer();
		content.append("<!DOCTYPE html>\n" + "<html lang=\"en\">\n" + "\n" + "<head>\n"
				+ "    <meta charset=\"UTF-8\">\n"
				+ "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
				+ "    <meta http-equiv=\"X-UA-Compatible\" content=\"ie=edge\">\n" + "    <title>Document</title>\n"
				+ "    <style>\n" + "@font-face {\n" + " font-family: squareSimple;\n"
				+ " src:url(https://www.gimcyun.com/gidvideo/fzzchjw.ttf);\n" + "}  .logo-text{\n"
				+ "font-size: 28px;\n" + "margin-bottom: 20px;\n" + "}  .email-page {\n" + "   padding: 30px 100px;\n"
				+ "    }\n" + "\n" + "    p {\n" + "        margin: 0;\n" + "        line-height: 26px;\n"
				+ "        color: #000;\n" + "        font-size: 16px;\n" + "    }\n" + "\n" + "    .logo {\n"
				+ "        margin-bottom: 30px;\n" + "    }\n" + "\n" + "    .link {\n" + "        margin: 30px 0;\n"
				+ "        width: 700px;\n" + "    }\n" + "\n" + "    .link a {\n" + "        background: #CCFFFF;\n"
				+ "        word-break: break-all\n" + "    }\n" + "\n" + "    a {\n" + "        color: #1C55FF;\n"
				+ "        text-decoration: underline\n" + "    }\n" + "    </style>\n" + "</head>\n" + "\n"
				+ "<body>\n" + "    <div class=\"email-page\" style=\"font-family: squareSimple;\">\n"
				+ "      <div class=\"logo-text\" >GIMC云邮箱验证</div>" + "        <p>您好，");

		content.append("，欢迎成为GIMC云用户！</br></p>" + "<p>请点击下面的链接对您的邮箱进行验证:</p>\n" + "            <a href=\"");
		content.append("\">");
		content.append("</a>\n" + "<p>（此链接24小时内有效，超时需要重新获取验证邮件）</p>        \n"
				+ "        <p>如果该链接无法点击，请将其复制粘贴到您的浏览器地址栏中访问。</p>" + "<p>若此邮件非您本人操作请忽略。</br></p>" + "<p>祝使用愉快！</br></p>"
				+ "        <p>GIMC云官网:\n"
                +"<img src='cid:logo' />"
				+ "            <a href=\"https://www.gimcyun.com\" target=\"_blank\">www.gimcyun.com</a>\n"
				+ "        </p>\n" + "    </div>\n" + "</body>\n" + "\n" + "</html>");
		MimeBodyPart text = new MimeBodyPart();
		text.setContent(content.toString(), "text/html;charset=gb2312");

		////添加图片
		MimeBodyPart img = new MimeBodyPart();

        DataSource ds3 = new FileDataSource("E:/data/logo.jpg");
        DataHandler dh3 = new DataHandler(ds3);

        img.setDataHandler(dh3);
		img.setContentID("logo");

		//添加图片和正文的关系
		MimeMultipart mimeMultipart = new MimeMultipart("related");
		mimeMultipart.addBodyPart(text);
		mimeMultipart.addBodyPart(img);
		message.setContent(mimeMultipart);
		message.saveChanges();

		// 3.发送邮件
		Transport.send(message);

	}

	public static void main(String[] args) throws Exception {
		SendMail SendMail = new SendMail("ymz@gimc.cn");
	}
}
