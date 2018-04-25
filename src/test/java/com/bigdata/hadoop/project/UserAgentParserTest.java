package com.bigdata.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;

/**
 * Created by RoninLee on 18-4-25.
 * UserAgentParserTest测试类
 */
public class UserAgentParserTest {

    public static void main(String[] args) {
        String source = "";
        //useragent工具类，可以通过读取日志某段信息拿到以下信息,对日志进行解析
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        String version = agent.getVersion();
        boolean isMobile = agent.isMobile();
    }
}
