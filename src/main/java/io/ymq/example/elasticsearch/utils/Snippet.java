package io.ymq.example.elasticsearch.utils;

import com.sun.nfs.XFileExtensionAccessor;
import com.sun.xfile.XFile;
import com.sun.xfile.XFileInputStream;
import com.sun.xfile.XFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class Snippet {
    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(Snippet.class);

    public void downloadViaNFS(final String ip, final String user,
                               final String password, final String dir) {

        System.out.println("NFS download begin!");

        try {
            String url = "nfs://" + ip + "/" + dir;
            System.out.println(url);
            XFile xf = new XFile(url);
            if (xf.exists()) {
                System.out.println("URL is OK!");
            } else {
                System.out.println("URL is Bad!");
                return;
            }
            XFileExtensionAccessor nfsx = (XFileExtensionAccessor) xf
                    .getExtensionAccessor();
            if (!nfsx.loginPCNFSD(ip, user, password)) {
                System.out.println("login failed!");
                return;
            }
            String[] fileList = xf.list();
            XFile temp = null;
            long startTime = System.currentTimeMillis();
            int filesz = 0;
            for (String file : fileList) {
                temp = new XFile(url + "/" + file);
                XFileInputStream in = new XFileInputStream(temp);
                XFileOutputStream out = new XFileOutputStream(temp + File.separator + file);
                int c;
                byte[] buf = new byte[8196];
                while ((c = in.read(buf)) > 0) {
                    filesz += c;
                    out.write(buf, 0, c);
                }
                logger.debug(file + " is downloaded!");
                in.close();
                out.close();
                if (temp.canWrite()) {
                    temp.delete();
                    logger.debug(file + " is deleted!");
                } else {
                    logger.debug(file + " can not be delted!");
                }
            }
            long endTime = System.currentTimeMillis();
            long timeDiff = endTime - startTime;
            int rate = (int) ((filesz / 1000) / (timeDiff / 1000.0));
            System.out.println("rate =" + rate);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        String ip = "192.168.10.123";
        String user = "es";
        String password = "";
        String dir = "opt/test";
        Snippet s = new Snippet();
        s.downloadViaNFS(ip, user, password, dir);
    }
}