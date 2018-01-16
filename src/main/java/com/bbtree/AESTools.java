package com.bbtree;


import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chenzhilei on 16/5/17.
 * 解密requestBody
 */
public class AESTools {
    private static final Logger logger = LoggerFactory.getLogger(AESTools.class);
    private static final String JSON_FORMAT_ERROR = "1";
    private static final String JSON_PARSE_ERROR = "2";
    private static final String JSON_UUID_MISS_ERROR = "3";
    private static final String JSON_DATA_MISS_ERROR = "4";
    private static final String DECRYPT_FAIL_ERROR = "5";
    private static final String BODY_IS_EMPTY = "0";


    public static String decrypt(String body) {
        if ("-".equals(body)) {
            return BODY_IS_EMPTY;
        }

        if (body.contains("%22")) {
            return StringTools.urlDCode(body);
        }
        //转化转义字符
        String json = StringTools.unicodeToString(body);
        //判断是否为JSON
        if (!(json.startsWith("{")) && !(json.startsWith("["))) {
            return JSON_FORMAT_ERROR;
        }
        //
        JsonNode rootNode = StringTools.parse_json(json);
        if (rootNode == null) {
            logger.error("JSON解析失败,源数据为" + json);
            return JSON_PARSE_ERROR;
        }
        String uuid = rootNode.path("uuid").getValueAsText();
        if (StringUtils.isBlank(uuid)) {
            return json;
        }
        String data = rootNode.path("data").getValueAsText();
        if (StringUtils.isBlank(data)) {
            return json;
        }
        boolean isIos = body.contains("ios_arm64_flag");
        logger.debug("解析JSON成功,uuid为:" + uuid + ";isIos为:" + isIos + ";data为:" + data);
        String keyCode = RedisTools.getKeyCode(uuid);
        String decrypt;
        try {
            decrypt = AESTools.decrypt(keyCode, data, isIos);
        } catch (Exception e) {
            logger.error("AES解密失败,uuid为:" + uuid + ";isIos为:" + isIos + ";data为:" + data + ";源数据为:" + body);
            return DECRYPT_FAIL_ERROR;
        }
        return decrypt;
    }


    public static String format(String body) {

        List<String> splitBody = splitBody(body);

        String requestBodyDecrypt = "";
        //检查字段数是否满足
        if (splitBody.size() < 19) {
            logger.debug("字段少于 19:" + body);
        } else if (isFilter(splitBody)) {
            return "";
        } else if (isUnEncrypt(splitBody)) {
            //检查是否未加密
            requestBodyDecrypt = splitBody.get(18);
        } else {
            logger.debug("字段满足条件:" + splitBody.get(18));
            requestBodyDecrypt = AESTools.decrypt(splitBody.get(18));
        }
        logger.debug("获得解密串为:" + requestBodyDecrypt);
        StringBuilder log = new StringBuilder();
//        for (String fields : splitBody) {
//            log.append(fields).append("\001");
//        }
        for (int i = 0; i < splitBody.size(); i++) {
            //排除无用字段
            if (i == 3 || (i > 6 && i < 16) || i == splitBody.size() - 1) {
                log.append("").append("\001");
            } else {
                log.append(splitBody.get(i)).append("\001");
            }
        }
        log.append(requestBodyDecrypt);
        return String.valueOf(log);
    }

    public static List<String> splitBody(String body) {
        String[] arrays = body.split("]");
        List<String> list = new ArrayList<String>();
        StringBuilder requestBody = new StringBuilder();
        for (int i = 0; i < arrays.length; i++) {
            if (i == 4) {
                arrays[i] = DateTools.format(arrays[i], "dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss");
            }
            if (i < 18) {
                list.add(arrays[i].trim().replaceAll("\\[", ""));
            } else {
                requestBody.append(arrays[i].trim());
            }
        }
        list.add(String.valueOf(requestBody).replaceAll("\\[", "").replaceAll("]", ""));
        return list;
    }

    public static String replaceDelimit(String body) {
        String[] arrays = body.split("]");
        StringBuilder sb = new StringBuilder();
        StringBuilder requestBody = new StringBuilder();
        for (int i = 0; i < arrays.length; i++) {
            if (i < 18) {
                sb.append(arrays[i].replaceAll("\\[", "")).append("\001");
            } else {
                requestBody.append(arrays[i]);
            }
        }
        sb.append(String.valueOf(requestBody).replaceAll("\\[", "").replaceAll("]", ""));
        return String.valueOf(sb);
    }


    private static final String AESTYPE_ZEROBYTEPADDING = "AES/ECB/ZeroBytePadding";
    private static final String AESTYPE_PKCS7PADDING = "AES/ECB/PKCS7Padding";

    static {
        Security.addProvider(new BouncyCastleProvider());
    }


    public static String decrypt(String key, String data, boolean isIos) throws Exception {
        byte[] decrypt;
        Cipher cipher;
        Key k;
        if (isIos) {
            k = generateKey(StringUtils.substring(StringUtils.rightPad(key, 16, '0'), 0, 16));
            cipher = Cipher.getInstance(AESTYPE_PKCS7PADDING);
        } else {
            k = generateKey(key);
            cipher = Cipher.getInstance(AESTYPE_ZEROBYTEPADDING);
        }
        cipher.init(Cipher.DECRYPT_MODE, k);
        decrypt = cipher.doFinal(Base64.decodeBase64(data.getBytes()));
        return new String(decrypt, "utf-8").trim();
    }

    private static Key generateKey(String key) throws Exception {
        return new SecretKeySpec(key.getBytes("utf-8"), "AES");
    }

    /**
     * 过滤的日志
     *
     * @return 是否过滤
     */
    private static boolean isFilter(List<String> str) {

        if (str.get(5).contains("/service/v2/statuses/upload")) {
            return true;
        }
        String host = str.get(0).trim();
        if (!(host.endsWith(".com") || host.endsWith(".cn") || host.endsWith(".net"))) {
            return true;
        }
        String requestBody = str.get(18);
        if (requestBody.length() > 65536) {
            return true;
        }


//        Pattern pattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
//        Matcher matcher = pattern.matcher(host);
//        !(matcher.find());
        return false;
    }

    public static String encrypt(String key, String data, boolean isIos) throws Exception {
        byte[] encrypt;

        Cipher cipher;
        Key k;
        if (isIos) {
            k = generateKey(StringUtils.substring(StringUtils.rightPad(key, 16, '0'), 0, 16));
            cipher = Cipher.getInstance(AESTYPE_PKCS7PADDING);
        } else {
            k = generateKey(key);
            cipher = Cipher.getInstance(AESTYPE_ZEROBYTEPADDING);
        }
        cipher.init(Cipher.ENCRYPT_MODE, k);
        encrypt = cipher.doFinal(data.getBytes("utf-8"));

        return new String(Base64.encodeBase64(encrypt));
    }

    private static final Pattern pattern = Pattern.compile("^(\\w+=\\w+|&)+");

    /**
     * 是否未加密的
     *
     * @return 是否未加密
     */
    private static boolean isUnEncrypt(List<String> splitBody) {
        boolean is_decrypt = false;
        String request = splitBody.get(5);
        if (request.contains("/service/v3/message/entrance/info")) {
            is_decrypt = true;
        }
        String requestBody = splitBody.get(18);
        Matcher matcher = pattern.matcher(requestBody);
        if (matcher.find()) {
            is_decrypt = true;
        }
        return is_decrypt;
    }
}
