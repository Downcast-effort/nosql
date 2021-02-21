package com.dandelion.nosql.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * @author dandelion
 * @version 1.0
 * @date 2021/1/23 15:39
 */
public class PropertyUtil<pulbic> {
    private static Logger log = LoggerFactory.getLogger(PropertyUtil.class);


    /**
     * 加载资源文件
     * @param path
     * @return
     */
    public static Properties load(String path){
        InputStream stream = findFile(path);
        if (stream == null){
            stream = findInResource(path);
        }
        if (null == stream){
            return null;
        }else{
            Properties properties = new Properties();
            try{
                properties.load(stream);
                return properties;
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 寻找文件
     * @param file
     * @return
     */
    public  static InputStream findFile(String file){
        String userDir = System.getProperty("user.dir");
        if (!StringUtils.isBlank(userDir)){
            File tmpF = new File(userDir,file);
            if (tmpF.exists() && tmpF.isFile()){
                try{
                    FileInputStream in = new FileInputStream(tmpF);
                    log.info("加载配置：" + tmpF.getAbsolutePath());
                    return in;
                }catch (FileNotFoundException e){
                    e.printStackTrace();
                    return null;
                }
            }
        }
        return null;
    }


    /**
     * @param file
     * @return
     */
    public static InputStream findInResource(String file){
        log.info("加载配置：classpath:/" + file);
        return PropertyUtil.class.getResourceAsStream("/" + file);
    }
}
