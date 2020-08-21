package Jan;


import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * ftp文件服务器工具类
 *
 * @author zhangjw
 * @date 2018-04-16 14:02
 **/
public class FTPUtil {

    private static Logger logger = LoggerFactory.getLogger(FTPUtil.class);

    /**
     * 连接ftp服务器
     * @param ftpHost FTP主机服务器
     * @param ftpPassword FTP 登录密码
     * @param ftpUserName FTP登录用户名
     * @param ftpPort FTP端口 默认为21
     * @return
     */
    public static FTPClient getFTPClient(String ftpHost, String ftpPassword, String ftpUserName, int ftpPort) {
        FTPClient ftpClient = null;
        try {
            ftpClient = new FTPClient();
            ftpClient.connect(ftpHost, ftpPort);// 连接FTP服务器
            ftpClient.login(ftpUserName, ftpPassword);// 登陆FTP服务器
            if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
                logger.info("未连接到FTP，用户名或密码错误。");
                ftpClient.disconnect();
            } else {
                logger.info("FTP连接成功。");
            }
        } catch (SocketException e) {
            logger.info("FTP的IP地址可能错误，请正确配置。");
        } catch (IOException e) {
            logger.info("FTP的端口错误,请正确配置。");
        }
        return ftpClient;
    }

    /**
     * 在服务器的FTP路径下上读取文件
     *
     * @param ftpUserName
     * @param ftpPassword
     * @param ftpPath
     * @return
     */
    public static List<String> readFileFromFTP(String ftpUserName, String ftpPassword, String ftpPath, String ftpHost,
                                               int ftpPort, String fileName) {
        InputStream in = null;
        FTPClient ftpClient = null;
        logger.info("开始读取" + ftpHost + ":" + ftpPort + "/" + ftpPath + "文件!");
        try {
            ftpClient = FTPUtil.getFTPClient(ftpHost, ftpPassword, ftpUserName, ftpPort);
            ftpClient.setControlEncoding("UTF-8"); // 中文支持
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            ftpClient.changeWorkingDirectory(ftpPath);
            in = ftpClient.retrieveFileStream(fileName);
        } catch (FileNotFoundException e) {
            logger.error("没有找到" + ftpPath + "文件");
        } catch (SocketException e) {
            logger.error("连接FTP失败.");
        } catch (IOException e) {
            logger.error("文件读取错误。");
        }
        List<String> result = new ArrayList<>();
        if (in != null) {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String data;
            try {
                while ((data = br.readLine()) != null) {
                    result.add(data);
                }
            } catch (IOException e) {
                logger.error("文件读取错误。");
            } finally {
                try {
                    ftpClient.disconnect();
                } catch (IOException e) {
                    logger.error("未知错误，{}", e.getMessage());
                }
            }
        } else {
            logger.error("in为空，不能读取。");
        }
        return result;
    }

    /**
     * 将文件下载到本地
     * @param url
     * @param port
     * @param username
     * @param password
     * @param remotePath
     * @param fileName
     * @param localPath
     * @return
     */
    public static boolean downFile(String url, int port, String username, String password, String remotePath,
                                   String fileName, String localPath) {
        logger.info("FTPUtil.downFile开始下载文件,remotePath为{},fileName为{},localPath为{}", remotePath, fileName, localPath);
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);
            //如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
            ftp.login(username, password);//登录
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            ftp.setBufferSize(1024*1024);//增大缓冲区
            ftp.enterLocalPassiveMode();
            ftp.changeWorkingDirectory(remotePath);//转移到FTP服务器目录
            FTPFile[] fs = ftp.listFiles();
            logger.info("读取到的文件FTPFile大小为{}", fs.length);
            for (FTPFile ff : fs) {
                if (ff.getName().equals(fileName)) {
                    //判断本地文件夹是否存在，不存在就新建文件夹
                    File dir = new File(localPath);
                    if (!dir.exists()) {
                        dir.mkdirs();
                    }
                    File localFile = new File(localPath + ff.getName());

                    OutputStream is = new FileOutputStream(localFile);
                    ftp.retrieveFile(ff.getName(), is);
                    is.close();
                    logger.info("下载文件到{}{}下，文件大小为{}", localPath, fileName, localFile.getTotalSpace());
                }
            }

            ftp.logout();
            success = true;
            logger.info("FTPUtil.downFile下载文件成功");
        } catch (IOException e) {
            logger.info("FTPUtil.downFile下载文件失败，原因为{}", e.getMessage());
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return success;
    }

    /**
     * Description: 向FTP服务器上传文件
     * @param ftpHost FTP服务器hostname
     * @param ftpUserName 账号
     * @param ftpPassword 密码
     * @param ftpPort 端口
     * @param ftpPath  FTP服务器中文件所在路径 格式： ftptest/aa
     * @param fileName ftp文件名称
     * @param localFilePath 文件名
     * @return 成功返回true，否则返回false
     */
    public static boolean uploadFile(String ftpHost, String ftpUserName, String ftpPassword, int ftpPort,
                                     String ftpPath, String fileName, String localFilePath) {
        boolean success = false;
        FTPClient ftpClient = new FTPClient();
        try {
            int reply;
            ftpClient.connect(ftpHost, ftpPort);
            //如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
            ftpClient.login(ftpUserName, ftpPassword);//登录
            reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                return success;
            }
            ftpClient.setControlEncoding("UTF-8"); // 中文支持
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            ftpClient.changeWorkingDirectory(ftpPath);

            File f = new File(localFilePath + fileName);
            InputStream in = new FileInputStream(f);
            ftpClient.storeFile(fileName, in);

            in.close();
            ftpClient.logout();
            success = true;
            //上传完成后删除本地文件
            f.delete();
            logger.info("FTP上传文件成功");
        } catch (IOException e) {
            logger.error("上传文件失败，原因为{}", e.getMessage());
        } finally {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect();
                } catch (IOException ioe) {
                    logger.error("上传文件失败，原因为{}", ioe.getMessage());
                }
            }
        }
        return success;
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        int ftpPort = 21;
        String ftpUserName = "eaglelog";
        String ftpPassword = "Eagle@Ftp2018";
        String ftpHost = "14.18.155.218";
        String ftpPath = "/csv/";

        //上传文件到ftp服务器
        //FTPUtil.uploadFile(ftpHost, ftpUserName, ftpPassword, ftpPort, ftpPath, "20180414142324.csv", "/csv/");
        //从ftp服务器下载文件
        FTPUtil.downFile(ftpHost, ftpPort, ftpUserName, ftpPassword, ftpPath, "20180604162600.csv", "G:\\test\\");
        logger.info("下载耗时{}", System.currentTimeMillis() - startTime);
    }
}
