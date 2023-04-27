package com.wix.mysql;

import com.wix.mysql.config.Charset;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.MysqldConfig.SystemDefaults;
import com.wix.mysql.exceptions.CommandFailedException;
import de.flapdoodle.embed.process.distribution.Platform;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.wix.mysql.utils.Utils.isNullOrEmpty;
import static de.flapdoodle.embed.process.distribution.Platform.Windows;
import static java.lang.String.format;

class MysqlClient {

    private final MysqldConfig config;
    private final MysqldExecutable executable;
    private final String schemaName;
    private final Charset effectiveCharset;

    public MysqlClient(
            final MysqldConfig config,
            final MysqldExecutable executable,
            final String schemaName,
            final Charset effectiveCharset) {
        this.config = config;
        this.executable = executable;
        this.schemaName = schemaName;
        this.effectiveCharset = effectiveCharset;
    }

    List<String> executeScripts(final List<SqlScriptSource> sqls) {
        List<String> res = new ArrayList<>(sqls.size());
        try {
            for (SqlScriptSource sql : sqls) {
                res.add(execute(sql.read()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    public List<String> executeCommands(final String... sqls) {
        List<String> res = new ArrayList<>(sqls.length);
        for (String sql : sqls) {
            res.add(execute(sql));
        }
        return res;
    }

    private String execute(final String sql) {
        String command = (Platform.detect() == Windows) ? format("\"%s\"", sql) : sql;
        String out = "";
        try {
            ProcessBuilder processBuilder;
            if (Platform.detect() == Windows) {
                processBuilder = new ProcessBuilder(Paths.get(executable.getBaseDir().getAbsolutePath(), "bin", "mysql").toString(),
                        "--protocol=tcp",
                        "--host=localhost",
                        "--password=",
                        format("--default-character-set=%s", effectiveCharset.getCharset()),
                        format("--user=%s", SystemDefaults.USERNAME),
                        format("--port=%s", config.getPort()),
                        schemaName);
            } else {
                processBuilder = new ProcessBuilder(Paths.get(executable.getBaseDir().getAbsolutePath(), "bin", "mysql").toString(),
                        "--protocol=socket",
                        "-S",
                        config.getSockFile(),
                        "--password=",
                        format("--default-character-set=%s", effectiveCharset.getCharset()),
                        format("--user=%s", SystemDefaults.USERNAME),
                        format("--port=%s", config.getPort()),
                        schemaName);
            }

            File errorFile = File.createTempFile("mysql", "error");
            File outputFile = File.createTempFile("mysql", "output");
            File inputFile = File.createTempFile("mysql", "input");
            inputFile.deleteOnExit();
            outputFile.deleteOnExit();
            errorFile.deleteOnExit();

            try (FileOutputStream stream = new FileOutputStream(inputFile)) {
                IOUtils.copy(new StringReader(sql), stream, StandardCharsets.UTF_8);
                stream.flush();
            }

            processBuilder = processBuilder.redirectError(errorFile)
                    .redirectOutput(outputFile)
                    .redirectInput(inputFile);

            Process p = processBuilder.start();

            if (p.waitFor() != 0) {

                try (FileInputStream stream = new FileInputStream(errorFile)) {
                    String err = IOUtils.toString(stream, StandardCharsets.UTF_8);

                    if (isNullOrEmpty(out))
                        throw new CommandFailedException(command, schemaName, p.waitFor(), err);
                    else
                        throw new CommandFailedException(command, schemaName, p.waitFor(), out);
                }
            }

            try (FileInputStream stream = new FileInputStream(outputFile)) {
                out = IOUtils.toString(stream, StandardCharsets.UTF_8);
            }

        } catch (IOException | InterruptedException e) {
            throw new CommandFailedException(command, schemaName, e.getMessage(), e);
        }
        return out;
    }
}
