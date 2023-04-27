package com.wix.mysql;

import com.wix.mysql.config.Charset;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.RuntimeConfigBuilder;
import com.wix.mysql.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.Slf4jLevel;
import junit.framework.TestCase;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static de.flapdoodle.embed.process.io.Processors.logTo;

public class MysqlClientTest extends TestCase {

    public void testThisApi() throws IOException {
        File errorLogFile = File.createTempFile("mysql", ".error.log");
        errorLogFile.deleteOnExit();
        MysqldConfig config = aMysqldConfig(Version.v5_7_latest)
                .withCharset(UTF8)
                .withFreePort()
//                .withServerVariable("log_error", errorLogFile.toString())
//                    .withUser("yc", "yc")
                .withTimeZone("Asia/Shanghai")
                .build();

//        EmbeddedMysql.Builder builder = anEmbeddedMysql(config);
        IStreamProcessor log = logTo(LoggerFactory.getLogger(MysqlClientTest.class), Slf4jLevel.INFO);

        EmbeddedMysql start = EmbeddedMysql.anEmbeddedMysql(
                config
        ).start((c1,c2) -> new RuntimeConfigBuilder()
                .defaults(c1, c2)
                .processOutput(new ProcessOutput(log, log, log))
                .build());

//        MysqldStarter mysqldStarter = new MysqldStarter(runtimeConfig);

//        new MysqlClient(config,mysqldStarter,)
        MysqldExecutable me = start.executable;
//        me.start();

        MysqlClient client = new MysqlClient(config, me, MysqldConfig.SystemDefaults.SCHEMA, UTF8);

        List<String> rs = client.executeCommands("select 1");

        System.out.println(rs);
        assertEquals(1, rs.size());
        assertEquals("1\n1\n", rs.get(0));

        System.out.println(client.executeCommands("show variables"));

        me.stop();
    }

}