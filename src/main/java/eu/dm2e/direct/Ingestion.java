/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.dm2e.direct;

import eu.dm2e.grafeo.Grafeo;
import eu.dm2e.grafeo.jena.GrafeoImpl;
import net.lingala.zip4j.core.ZipFile;
import net.sf.saxon.Controller;
import net.sf.saxon.serialize.MessageEmitter;
import org.apache.commons.cli.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A direct ingestion tool to ingest XML data via an XSLT mapping
 * into a triple store.
 *
 * @author Dominique Ritze, Kai Eckert
 */

public class Ingestion {

    public final static String TOOL_URI = "http://data.dm2e.eu/data/tools/dm2e-direct";
    public final static String VERSION = "1.0";

    protected Logger log = LoggerFactory.getLogger(getClass().getName());

    String xslt;
    Properties xsltProps = new Properties();
    String originalXslt;
    List<String> originalInputs = new ArrayList<String>();
    String endpointUpdate;
    String endpointSelect;
    String provider;
    String dataset;
    String base;
    String graphName;
    String version;
    String datasetURI;
    String label;
    FileWriter xslLog;
    FileWriter ingestionLog;
    List<String> include = new ArrayList<String>();
    Set<String> exclude = new HashSet<String>();

    long fileCount = 0;
    boolean useOAIPMH = false;
    boolean keepTemp = false;


    public static void main(String args[]) {


        Options options = new Options();
        options.addOption("c", "config", true, "The direct configuration to be used.");
        options.addOption("p", "provider", true, "The provider ID that is used to create your URIs, e.g., mpiwg.");
        options.addOption("d", "dataset", true, "The dataset ID this is used to create your URIs, e.g., codices.");
        options.addOption("l", "label", true, "A label describing the ingested dataset.");
        options.addOption("x", "xslt", true, "The XSLT mapping (zipped, a folder or a file)");
        options.addOption("xp", "xslt-props", true, "The file containing properties passed to the XSLT process.");
        options.addOption("pmh", "use-oai-pmh", true, "Set to true, if input is an OAI-PMH endpoint.");
        options.addOption("i", "include", true, "comma-separated list of identifiers or files to be included");
        options.addOption("e", "exclude", true, "comma-separated list of identifiers or files to be excluded");
        options.addOption("kt", "keep-temporary", true, "Keep temporary files (in default temp directory) for debugging. ");
        options.addOption("h", "help", false, "Show this help.");

        CommandLineParser clp = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = clp.parse(options, args);
        } catch (ParseException pe) {
            HelpFormatter help = new HelpFormatter();
            System.err.println("Error: " + pe.getMessage());
            System.err.println();
            help.printHelp("ingest", options);
            return;
        }

        if (cmd.hasOption("h")) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("ingest", options);
            return;

        }


        Properties properties = new Properties();
        File defaultConfig = new File("default.properties");
        if (defaultConfig.exists()) {
            System.out.println("Loading default configuration: default.properties");
            try {
                properties.load(new FileInputStream(defaultConfig));
            } catch (IOException e) {
                System.err.println("Error reading default config: " + e.getMessage());
            }
        }
        if (cmd.hasOption("c")) {
            System.out.println("Loading custom configuration: " + cmd.getOptionValue("c"));
            try {
                properties.load(new FileInputStream(cmd.getOptionValue("c")));
            } catch (IOException e) {
                System.err.println("Error reading custom config: " + e.getMessage());
            }
        }

        for (Option o : cmd.getOptions()) {
            properties.put(o.getLongOpt(), o.getValue());
        }

        if (cmd.getArgs().length == 0 && properties.getProperty("input") == null) {
            System.out.println("No input files. Either add them to a config or pass as argument: ");
            HelpFormatter help = new HelpFormatter();
            help.printHelp("ingest", options);
            return;
        }

        System.out.println("Configuration used: ");
        properties.list(System.out);

        new Ingestion().ingest(cmd, properties);

    }

    public void ingest(CommandLine cmd, Properties properties) {
        long start = System.currentTimeMillis();
        try {
            ingestionLog = new FileWriter(new File("ingestion.log"), true);
            ingestionLog.write("New log file\n");
            log("Check logging: ok");
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
        xslt = properties.getProperty("xslt");
        if (properties.getProperty("xslt-props") != null) {
            try {
                xsltProps.load(new FileInputStream(properties.getProperty("xslt-props")));
            } catch (IOException e) {
                throw new RuntimeException("An exception occurred: " + e, e);
            }
        }
        originalXslt = xslt;
        if (properties.get("use-oai-pmh") != null && properties.get("use-oai-pmh").equals("true")) {
            System.out.println("Using OAI-PMH mode for ingestion.");
            useOAIPMH = true;
        }
        if (properties.get("keep-temporary") != null && properties.get("keep-temporary").equals("true")) {
            System.out.println("Keeping temporary files. WARNING: Do not forget to delete them yourself.");
            keepTemp = true;
        }
        if (properties.get("exclude") != null) {
            for (String s : properties.get("exclude").toString().split(",")) {
                exclude.add(s);
            }
        }
        if (properties.get("include") != null) {
            for (String s : properties.get("include").toString().split(",")) {
                include.add(s);
            }
        }
        endpointUpdate = properties.getProperty("endpointUpdate");
        endpointSelect = properties.getProperty("endpointSelect");
        provider = properties.getProperty("provider");
        dataset = properties.getProperty("dataset");
        base = properties.getProperty("base");
        label = properties.getProperty("label");
        datasetURI = base + provider.toLowerCase() + "/" + dataset.toLowerCase();
        version = "" + DateTime.now().getMillis();
        graphName = datasetURI + "/" + version;
        try {
            xslLog = new FileWriter(new File("xsl.log"), true);
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
        System.out.println("See xsl.log for messages from the XSLT process.");
        System.out.println("Data will be ingested to dataset: " + graphName);

        if (xslt != null) {
            System.out.println("XSLT Stylesheet: " + xslt);

            xslt = prepareInput(xslt);
            xslt = grepRootStylesheet(xslt);

            long setupTime = System.currentTimeMillis() - start;
            System.out.println("Time for XSLT setup in sec: " + ((double) setupTime) / 1000);
        } else {
            System.out.println("No XSLT transformation configured.");
        }
        start = System.currentTimeMillis();

        if (cmd.getArgs().length == 0) {
            String input = properties.getProperty("input");
            originalInputs.add(input);
            input = useOAIPMH ? input : prepareInput(input);
            long args = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            System.out.println("No command line arguments, processing configured input.");
            System.out.println("Time for default data setup in sec: " + ((double) args) / 1000);
            processFileOrFolder(input);
        }


        for (String input : cmd.getArgs()) {
            long args = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            System.out.println("Time for data setup in sec: " + ((double) args) / 1000);
            System.out.println("Processing: " + input);
            originalInputs.add(input);
            input = useOAIPMH ? input : prepareInput(input);
            processFileOrFolder(input);
        }
        long end = System.currentTimeMillis() - start;
        System.out.println("\nTime for transformation in sec: " + ((double) end) / 1000);


    }

    protected void xslLog(String message) {
        try {
            xslLog.write(message);
            xslLog.write("\n");
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

    }

    protected void log(String message) {
        try {
            ingestionLog.write(new java.text.SimpleDateFormat("MM/dd hh:mm:ss").format(new Date()) + ": " + message);
            ingestionLog.write("\n");
            ingestionLog.flush();
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

    }

    protected void log(Throwable t) {
        try {
            ingestionLog.write(new java.text.SimpleDateFormat("MM/dd hh:mm:ss").format(new Date()) + ": " + t.getMessage());
            ingestionLog.flush();
            t.printStackTrace(new PrintWriter(ingestionLog));
            ingestionLog.flush();
            ingestionLog.write("\n");
            ingestionLog.flush();
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

    }


    public void processFile(String input) {
        try {
            xslLog("===============================");
            xslLog("Date: " + new Date());
            xslLog("File: " + input);

            File tmp = new File(input);
            if (xslt != null) {
                File f = new File(input);
                StreamSource xslSource = new StreamSource(xslt);
                xslSource.setSystemId(xslt);

                StreamSource xmlSource = new StreamSource(f);
                xmlSource.setSystemId(f);

                try {
                    tmp = File.createTempFile("TRANS_", ".xml");
                } catch (IOException e) {
                    log(e.getMessage());
                    throw new RuntimeException("An exception occurred: " + e, e);
                }
                if (!keepTemp) tmp.deleteOnExit();
                FileWriter resultXML = null;
                try {
                    resultXML = new FileWriter(tmp);
                } catch (IOException e) {
                    log(e.getMessage());
                    throw new RuntimeException("An exception occurred: " + e, e);
                }
                TransformerFactory transFact = new net.sf.saxon.TransformerFactoryImpl();

                try {
                    Transformer trans = transFact.newTransformer(
                            new StreamSource(xslt));
                    MessageEmitter emitter = new MessageEmitter();
                    emitter.setWriter(xslLog);
                    ((net.sf.saxon.Controller) trans).setMessageEmitter(emitter);
                    for (String key : xsltProps.stringPropertyNames()) {
                        ((Controller) trans).setParameter(key, xsltProps.get(key));
                    }
                    trans.transform(new StreamSource(f),
                            new StreamResult(resultXML));
                } catch (TransformerException e) {
                    log.error("XSLT: " + xslt);
                    log.error("File: " + f.toString());

                    log(e.getMessage());
                    throw new RuntimeException("An exception occurred: " + e, e);
                }
                try {
                    resultXML.flush();
                    resultXML.close();
                } catch (IOException e) {
                    log(e.getMessage());
                    throw new RuntimeException("An exception occurred: " + e, e);

                }
            }
            Grafeo g = new GrafeoImpl(tmp);
            g.postToEndpoint(endpointUpdate, graphName);
            System.out.print(".");
            fileCount++;
            if (fileCount % 50 == 0) {
                System.out.println("   " + fileCount);
            }
        } catch (Throwable t) {
            log(t);
            System.out.print("x");
            fileCount++;
            if (fileCount % 50 == 0) {
                System.out.println("   " + fileCount);
            }
            throw new RuntimeException(t);

        }


    }


    public String grepRootStylesheet(String zipdir) {
        if (!new File(zipdir).isDirectory()) return zipdir;
        Pattern pattern = Pattern.compile("xsl:template match=\"/\"");
        for (String file : DataTool.getFiles(zipdir)) {

            Scanner scanner = null;
            try {
                scanner = new Scanner(new File(file));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("An exception occurred: " + e, e);
            } finally {
            	scanner.close();
            }
            if (scanner.findWithinHorizon(pattern, 0) != null) return file;
        }

        return null;

    }

    public void processFileOrFolder(String input) {

        IngestionActivity activity = new IngestionActivity();
        activity.setId(graphName + "/ingestion");
        activity.setAgent(URI.create(TOOL_URI + "/" + VERSION));
        if (originalXslt != null && (originalXslt.startsWith("http") || originalXslt.startsWith("ftp"))) {
            activity.getInputs().add(URI.create(originalXslt));
        } else if (originalXslt != null) {
            activity.setXslt(new File(originalXslt).toURI());
        }
        for (String i : originalInputs) {
            if (i.startsWith("http") || i.startsWith("ftp")) {
                activity.getInputs().add(URI.create(i));
            } else {
                activity.getInputs().add(new File(i).toURI());
            }

        }


        VersionedDatasetPojo ds = new VersionedDatasetPojo();
        ds.setId(graphName);
        ds.setLabel(label != null ? label : dataset);
        ds.setComment("XSLT: " + originalXslt + " Input: " + input + " Generated: " + new Date() + " by DM2E Direct Ingestion.");
        ds.setTimestamp(DateTime.now());
        ds.setDatasetID(URI.create(datasetURI));
        ds.findLatest(endpointSelect);
        ds.setJobURI(URI.create(activity.getId()));


        Grafeo g = new GrafeoImpl();
        g.getObjectMapper().addObject(ds);
        g.getObjectMapper().addObject(activity);
        g.postToEndpoint(endpointUpdate, graphName);
        log.info("Provenance: " + g.getTerseTurtle());
        System.out.println("Provenance written.");
        System.out.println("Check result at: http://lelystad.informatik.uni-mannheim.de:3000/direct/html/dataset/" + provider + "/" + dataset + "/" + version);

        List<String> errors = new ArrayList<String>();
        if (!useOAIPMH) {
            File f = new File(input);
            if (f.isFile()) {
                try {
                    processFile(input);
                } catch (Throwable t) {
                    log(t);
                    log.error("\n Ingestion Error: " + t.getMessage(), t);
                    errors.add(input);
                }
            } else if (f.isDirectory()) {
                for (String file : DataTool.getFiles(f.toString())) {
                    try {
                        processFile(file);
                    } catch (Throwable t) {
                        log(t);
                        log.error("\n Ingestion Error: " + t.getMessage(), t);
                        errors.add(input);
                    }

                }

            }
        } else {

            PMHarvester harvester = new PMHarvester(input);
            List<String> todo = include.isEmpty() ? harvester.getIdentifiers() : include;
            for (String id : todo) {
                if (exclude.contains(id)) continue;
                String target = harvester.getRecord(id);
                target = DataTool.download(target);
                try {
                    processFile(target);
                } catch (Throwable t) {
                    log.error("\n Ingestion Error: " + t.getMessage(), t);
                    log(t);
                    errors.add(id);
                }

            }
        }
        System.out.println("Inputs in error: ");
        for (String err : errors) {
            System.out.println(err);
        }


    }

    public String prepareInput(String input) {
        if (new File(input).isDirectory()) return input;
        if (input.startsWith("http") || input.startsWith("ftp")) {
            input = DataTool.download(input);
        }
        try {
            if (new ZipFile(input).isValidZipFile()) {
                input = DataTool.unzip(input);
            }
        } catch (net.lingala.zip4j.exception.ZipException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

        return input;
    }


}
