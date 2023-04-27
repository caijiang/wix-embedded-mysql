package com.wix.mysql.config;

import com.wix.mysql.distribution.Version;
import de.flapdoodle.embed.process.config.ExecutableProcessConfig;
import de.flapdoodle.embed.process.config.ISupportConfig;
import de.flapdoodle.embed.process.distribution.IVersion;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MysqldConfig extends ExecutableProcessConfig {

    private final int port;
    private final Charset charset;
    private final User user;
    private final TimeZone timeZone;
    private final Timeout timeout;
    private final List<ServerVariable> serverVariables;
    private final String tempDir;
    private File sockFile;

    protected MysqldConfig(
            final IVersion version,
            final int port,
            final Charset charset,
            final User user,
            final TimeZone timeZone,
            final Timeout timeout,
            final List<ServerVariable> serverVariables,
            final String tempDir) {
        super(version, new ISupportConfig() {
            public String getName() {
                return "mysqld";
            }

            public String getSupportUrl() {
                return "https://github.com/wix/wix-embedded-mysql/issues";
            }

            public String messageOnException(Class<?> context, Exception exception) {
                return "no message";
            }
        });

        if (user.name.equals("root")) {
            throw new IllegalArgumentException("Usage of username 'root' is forbidden as it's reserved for system use");
        }

        this.port = port;
        this.charset = charset;
        this.user = user;
        this.timeZone = timeZone;
        this.timeout = timeout;
        this.serverVariables = serverVariables;
        this.tempDir = tempDir;
    }

    public Version getVersion() {
        return (Version) version;
    }

    public Charset getCharset() {
        return charset;
    }

    public int getPort() {
        return port;
    }

    public long getTimeout(TimeUnit target) {
        return this.timeout.to(target);
    }

    public String getUsername() {
        return user.name;
    }

    public String getPassword() {
        return user.password;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public List<ServerVariable> getServerVariables() {
        return serverVariables;
    }

    public String getTempDir() {
        return tempDir;
    }

    public static Builder aMysqldConfig(final Version version) {
        return new Builder(version);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    /**
     * Helper for getting stable sock classPathFile. Saving to local instance variable on service start does not work due
     * to the way flapdoodle process library works - it does all init in {@link de.flapdoodle.embed.process.runtime.AbstractProcess} and instance of
     * {@link com.wix.mysql.MysqldProcess} is not yet present, so vars are not initialized.
     * This algo gives stable sock classPathFile based on single executeCommands profile, but can leave trash sock classPathFiles in tmp dir.
     * <p>
     * Notes:
     * .sock classPathFile needs to be in system temp dir and not in ex. target/...
     * This is due to possible problems with existing mysql installation and apparmor profiles
     * in linuxes.
     */
    public String generateSockFile() {
        try {
            sockFile = File.createTempFile("mysql", ".sock");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return getSockFile();
    }

    public String getSockFile(){
        if (sockFile==null)
            return null;
        return sockFile.toString();
    }

    public static class Builder {
        private IVersion version;
        private int port = 3310;
        private Charset charset = Charset.defaults();
        private User user = new User("auser", "sa");
        private TimeZone timeZone = TimeZone.getTimeZone("UTC");
        private Timeout timeout = new Timeout(30, SECONDS);
        private final List<ServerVariable> serverVariables = new ArrayList<>();
        private String tempDir = "target/";

        public Builder(IVersion version) {
            this.version = version;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withFreePort() throws IOException {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                return withPort(socket.getLocalPort());
            }
        }

        public Builder withTimeout(long length, TimeUnit unit) {
            this.timeout = new Timeout(length, unit);
            return this;
        }

        public Builder withCharset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public Builder withUser(String username, String password) {
            this.user = new User(username, password);
            return this;
        }

        public Builder withTimeZone(TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder withTimeZone(String timeZoneId) {
            return withTimeZone(TimeZone.getTimeZone(timeZoneId));
        }

        /**
         * Provide mysql server option
         * <p>
         * See <a href="mysqld-option-tables">http://dev.mysql.com/doc/refman/5.7/en/mysqld-option-tables.html</a>
         */
        public Builder withServerVariable(String name, boolean value) {
            serverVariables.add(new ServerVariable<>(name, value));
            return this;
        }

        /**
         * Provide mysql server int variable
         * <p>
         * See <a href="mysqld-option-tables">http://dev.mysql.com/doc/refman/5.7/en/mysqld-option-tables.html</a>
         */
        public Builder withServerVariable(String name, int value) {
            serverVariables.add(new ServerVariable<>(name, value));
            return this;
        }

        /**
         * Provide mysql server string or enum variable
         * <p>
         * See <a href="mysqld-option-tables">http://dev.mysql.com/doc/refman/5.7/en/mysqld-option-tables.html</a>
         */
        public Builder withServerVariable(String name, String value) {
            serverVariables.add(new ServerVariable<>(name, value));
            return this;
        }

        public Builder withTempDir(String tempDir) {
            this.tempDir = tempDir;
            return this;
        }


        public MysqldConfig build() {
            return new MysqldConfig(version, port, charset, user, timeZone, timeout, serverVariables, tempDir);
        }
    }

    private static class User {
        private final String name;
        private final String password;

        User(String name, String password) {
            this.name = name;
            this.password = password;
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toStringExclude(this, "password");
        }
    }

    private static class Timeout {
        private final long length;
        private final TimeUnit unit;

        Timeout(long length, TimeUnit unit) {
            this.length = length;
            this.unit = unit;
        }

        long to(TimeUnit target) {
            return target.convert(length, unit);
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    public static class ServerVariable<T> {
        private final String name;
        private final T value;

        ServerVariable(final String name, final T value) {
            this.name = name;
            this.value = value;
        }

        public String toCommandLineArgument() {
            return String.format("--%s=%s", name, value);
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    public static class SystemDefaults {
        public final static String USERNAME = "root";
        public final static String SCHEMA = "information_schema";
    }

}
