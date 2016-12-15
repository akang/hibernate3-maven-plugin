package org.hibernate.tool.hbm2ddl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.NamingStrategy;
import org.hibernate.cfg.Settings;
import org.hibernate.dialect.Dialect;
import org.hibernate.jdbc.util.FormatStyle;
import org.hibernate.jdbc.util.Formatter;
import org.hibernate.jdbc.util.SQLStatementLogger;
import org.hibernate.util.ConfigHelper;
import org.hibernate.util.JDBCExceptionReporter;
import org.hibernate.util.PropertiesHelper;
import org.hibernate.util.ReflectHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 *
 * A modified version of the SchemaExport class that allows users to optionally exclude tables from hbm2ddl operations
 * @author Allan Kenneth Ang
 */
public class ConditionalSchemaExport{
    private static Logger log =  Logger.getLogger(ConditionalSchemaExport.class.getName());
    private ConnectionHelper connectionHelper;
    private String[] dropSQL;
    private String[] createSQL;
    private String outputFile;
    private String importFile;
    private Dialect dialect;
    private String delimiter;
    private final List exceptions;
    private boolean haltOnError;
    private Formatter formatter;
    private SQLStatementLogger sqlStatementLogger;



    public void setTablesToExclude(List<String> tablesToExclude) {
        dropSQL = filterSQLForExclusions(tablesToExclude,dropSQL);
        createSQL = filterSQLForExclusions(tablesToExclude,createSQL);

    }

    private String[] filterSQLForExclusions(List<String> tablesToExclude, String[] sqlArray) {
        //remove the exclusion tables from the drop and create sql scripts
        List<String> filteredDropSQL = new ArrayList();

        for(String sql:sqlArray){
            Boolean matchFound = false;
            for(String exclusionTable: tablesToExclude){
                if(sql.toLowerCase().indexOf(exclusionTable.toLowerCase())>=0){
                    log.info("Excluding: " + exclusionTable.toUpperCase());
                    matchFound = true;
                    break;
                }
            }

            if(!matchFound){
                filteredDropSQL.add(sql);
            }
        }

        return filteredDropSQL.toArray(new String[0]);
    }

    public ConditionalSchemaExport(Configuration cfg) throws HibernateException {
        this(cfg, cfg.getProperties());
    }

    public ConditionalSchemaExport(Configuration cfg, Settings settings) throws HibernateException {
        this.outputFile = null;
        this.importFile = "/import.sql";
        this.exceptions = new ArrayList();
        this.haltOnError = false;
        this.dialect = settings.getDialect();
        this.connectionHelper = new SuppliedConnectionProviderConnectionHelper(settings.getConnectionProvider());
        this.dropSQL = cfg.generateDropSchemaScript(this.dialect);
        this.createSQL = cfg.generateSchemaCreationScript(this.dialect);
        this.sqlStatementLogger = settings.getSqlStatementLogger();
        this.formatter = (this.sqlStatementLogger.isFormatSql()?FormatStyle.DDL:FormatStyle.NONE).getFormatter();
    }

    /** @deprecated */
    public ConditionalSchemaExport(Configuration cfg, Properties properties) throws HibernateException {
        this.outputFile = null;
        this.importFile = "/import.sql";
        this.exceptions = new ArrayList();
        this.haltOnError = false;
        this.dialect = Dialect.getDialect(properties);
        Properties props = new Properties();
        props.putAll(this.dialect.getDefaultProperties());
        props.putAll(properties);
        this.connectionHelper = new ManagedProviderConnectionHelper(props);
        this.dropSQL = cfg.generateDropSchemaScript(this.dialect);
        this.createSQL = cfg.generateSchemaCreationScript(this.dialect);
        this.formatter = (PropertiesHelper.getBoolean("hibernate.format_sql", props)?FormatStyle.DDL:FormatStyle.NONE).getFormatter();
    }

    public ConditionalSchemaExport(Configuration cfg, Connection connection) throws HibernateException {
        this.outputFile = null;
        this.importFile = "/import.sql";
        this.exceptions = new ArrayList();
        this.haltOnError = false;
        this.connectionHelper = new SuppliedConnectionHelper(connection);
        this.dialect = Dialect.getDialect(cfg.getProperties());
        this.dropSQL = cfg.generateDropSchemaScript(this.dialect);
        this.createSQL = cfg.generateSchemaCreationScript(this.dialect);
        this.formatter = (PropertiesHelper.getBoolean("hibernate.format_sql", cfg.getProperties())?FormatStyle.DDL:FormatStyle.NONE).getFormatter();
    }

    public ConditionalSchemaExport setOutputFile(String filename) {
        this.outputFile = filename;
        return this;
    }

    public ConditionalSchemaExport setImportFile(String filename) {
        this.importFile = filename;
        return this;
    }

    public ConditionalSchemaExport setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public ConditionalSchemaExport setFormat(boolean format) {
        this.formatter = (format?FormatStyle.DDL:FormatStyle.NONE).getFormatter();
        return this;
    }

    public ConditionalSchemaExport setHaltOnError(boolean haltOnError) {
        this.haltOnError = haltOnError;
        return this;
    }

    public void create(boolean script, boolean export) {
        this.execute(script, export, false, false);
    }

    public void drop(boolean script, boolean export) {
        this.execute(script, export, true, false);
    }

    public void execute(boolean script, boolean export, boolean justDrop, boolean justCreate) {
        log.info("Running hbm2ddl schema export");
        Connection connection = null;
        FileWriter outputFileWriter = null;
        InputStreamReader importFileReader = null;
        Statement statement = null;
        this.exceptions.clear();

        try {
            try {
                InputStream e = ConfigHelper.getResourceAsStream(this.importFile);
                importFileReader = new InputStreamReader(e);
            } catch (HibernateException var24) {
                log.info("import file not found: " + this.importFile);
            }

            if(this.outputFile != null) {
                log.info("writing generated schema to file: " + this.outputFile);
                outputFileWriter = new FileWriter(this.outputFile);
            }

            if(export) {
                log.info("exporting generated schema to database");
                this.connectionHelper.prepare(true);
                connection = this.connectionHelper.getConnection();
                statement = connection.createStatement();
            }

            if(!justCreate) {
                this.drop(script, export, outputFileWriter, statement);
            }

            if(!justDrop) {
                this.create(script, export, outputFileWriter, statement);
                if(export && importFileReader != null) {
                    this.importScript(importFileReader, statement);
                }
            }

            log.info("schema export complete");
        } catch (Exception var25) {
            this.exceptions.add(var25);
            log.severe("schema export unsuccessful: " + var25);
        } finally {
            try {
                if(statement != null) {
                    statement.close();
                }

                if(connection != null) {
                    this.connectionHelper.release();
                }
            } catch (Exception var23) {
                this.exceptions.add(var23);
                log.severe("Could not close connection: " + var23);
            }

            try {
                if(outputFileWriter != null) {
                    outputFileWriter.close();
                }

                if(importFileReader != null) {
                    importFileReader.close();
                }
            } catch (IOException var22) {
                this.exceptions.add(var22);
                log.severe("Error closing output file: " + this.outputFile + ": " + var22);
            }

        }

    }

    private void importScript(Reader importFileReader, Statement statement) throws IOException {
        log.info("Executing import script: " + this.importFile);
        BufferedReader reader = new BufferedReader(importFileReader);
        long lineNo = 0L;

        for(String sql = reader.readLine(); sql != null; sql = reader.readLine()) {
            try {
                ++lineNo;
                String e = sql.trim();
                if(e.length() != 0 && !e.startsWith("--") && !e.startsWith("//") && !e.startsWith("/*")) {
                    if(e.endsWith(";")) {
                        e = e.substring(0, e.length() - 1);
                    }

                    log.info(e);
                    statement.execute(e);
                }
            } catch (SQLException var8) {
                throw new JDBCException("Error during import script execution at line " + lineNo, var8);
            }
        }

    }

    private void create(boolean script, boolean export, Writer fileOutput, Statement statement) throws IOException {
        for(int j = 0; j < this.createSQL.length; ++j) {
            try {
                this.execute(script, export, fileOutput, statement, this.createSQL[j]);
            } catch (SQLException var7) {
                if(this.haltOnError) {
                    throw new JDBCException("Error during DDL export", var7);
                }

                this.exceptions.add(var7);
                log.severe("Unsuccessful: " + this.createSQL[j]);
                log.severe(var7.getMessage());
            }
        }

    }

    private void drop(boolean script, boolean export, Writer fileOutput, Statement statement) throws IOException {
        for(int i = 0; i < this.dropSQL.length; ++i) {
            try {
                this.execute(script, export, fileOutput, statement, this.dropSQL[i]);
            } catch (SQLException var7) {
                this.exceptions.add(var7);
                log.severe("Unsuccessful: " + this.dropSQL[i]);
                log.severe(var7.getMessage());
            }
        }

    }

    private void execute(boolean script, boolean export, Writer fileOutput, Statement statement, String sql) throws IOException, SQLException {
        String formatted = this.formatter.format(sql);
        if(this.delimiter != null) {
            formatted = formatted + this.delimiter;
        }

        if(script) {
            System.out.println(formatted);
        }

        log.info(formatted);
        if(this.outputFile != null) {
            fileOutput.write(formatted + "\n");
        }

        if(export) {
            statement.executeUpdate(sql);

            try {
                SQLWarning sqle = statement.getWarnings();
                if(sqle != null) {
                    JDBCExceptionReporter.logAndClearWarnings(this.connectionHelper.getConnection());
                }
            } catch (SQLException var8) {
                log.warning("unable to log SQLWarnings : " + var8);
            }
        }

    }

    public static void main(String[] args) {
        try {
            Configuration e = new Configuration();
            boolean script = true;
            boolean drop = false;
            boolean create = false;
            boolean halt = false;
            boolean export = true;
            String outFile = null;
            String importFile = "/import.sql";
            String propFile = null;
            boolean format = false;
            String delim = null;

            for(int se = 0; se < args.length; ++se) {
                if(args[se].startsWith("--")) {
                    if(args[se].equals("--quiet")) {
                        script = false;
                    } else if(args[se].equals("--drop")) {
                        drop = true;
                    } else if(args[se].equals("--create")) {
                        create = true;
                    } else if(args[se].equals("--haltonerror")) {
                        halt = true;
                    } else if(args[se].equals("--text")) {
                        export = false;
                    } else if(args[se].startsWith("--output=")) {
                        outFile = args[se].substring(9);
                    } else if(args[se].startsWith("--import=")) {
                        importFile = args[se].substring(9);
                    } else if(args[se].startsWith("--properties=")) {
                        propFile = args[se].substring(13);
                    } else if(args[se].equals("--format")) {
                        format = true;
                    } else if(args[se].startsWith("--delimiter=")) {
                        delim = args[se].substring(12);
                    } else if(args[se].startsWith("--config=")) {
                        e.configure(args[se].substring(9));
                    } else if(args[se].startsWith("--naming=")) {
                        e.setNamingStrategy((NamingStrategy)ReflectHelper.classForName(args[se].substring(9)).newInstance());
                    }
                } else {
                    String filename = args[se];
                    if(filename.endsWith(".jar")) {
                        e.addJar(new File(filename));
                    } else {
                        e.addFile(filename);
                    }
                }
            }

            if(propFile != null) {
                Properties var15 = new Properties();
                var15.putAll(e.getProperties());
                var15.load(new FileInputStream(propFile));
                e.setProperties(var15);
            }

            SchemaExport var16 = (new SchemaExport(e)).setHaltOnError(halt).setOutputFile(outFile).setImportFile(importFile).setDelimiter(delim);
            if(format) {
                var16.setFormat(true);
            }

            var16.execute(script, export, drop, create);
        } catch (Exception var14) {
            log.severe("Error creating schema: " + var14);
            var14.printStackTrace();
        }

    }

    public List getExceptions() {
        return this.exceptions;
    }


}
