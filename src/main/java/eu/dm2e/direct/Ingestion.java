/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.dm2e.direct;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.lingala.zip4j.core.ZipFile;
import net.sf.saxon.Controller;
import net.sf.saxon.serialize.MessageEmitter;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import eu.dm2e.grafeo.Grafeo;
import eu.dm2e.grafeo.jena.GrafeoImpl;
import eu.dm2e.grafeo.jena.SparqlSelect;
import eu.dm2e.validation.Dm2eValidationReport;
import eu.dm2e.validation.Dm2eValidator;
import eu.dm2e.validation.ValidationException;
import eu.dm2e.validation.ValidationLevel;
import eu.dm2e.validation.validator.Dm2eSpecificationVersion;

/**
 * A direct ingestion tool to ingest XML data via an XSLT mapping
 * into a triple store.
 *
 * @author Dominique Ritze, Kai Eckert
 */

public class Ingestion {

    public final static String TOOL_URI = "http://data.dm2e.eu/data/tools/dm2e-direct";
    public final static String VERSION = "1.1";

    private static final Logger log = LoggerFactory.getLogger(Ingestion.class);

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
    Dm2eValidator validator;
    FileWriter xslLog;
    FileWriter ingestionLog;
    File xslLogFile;
    File ingestionLogFile;
    List<String> include = new ArrayList<String>();
    Set<String> exclude = new HashSet<String>();
    ValidationLevel validationLevel = ValidationLevel.FATAL;
    String dm2eModelVersion;

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
        options.addOption("v", "validation", true, "Validate and stop at specified level. One of FATAL, ERROR, WARNING, NOTICE, OFF. Default: ERROR ");
        options.addOption("dm2e", "dm2e-model-version", true, "DM2E Model Version. Leave empty to get a list of supported versions.");
        // The following are by design only usable as command line parameters!
        options.addOption("delV", "delete-version", true, "Graph URI. Delete this ingestion (USE WITH CARE!)");
        options.addOption("delA", "delete-all-versions", true, "Collection URI. Delete all ingestions for this collection (USE WITH CARE!!!!)");
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
        InputStream defaultConfigStream = Ingestion.class.getResourceAsStream("default.properties");
        if (defaultConfigStream!=null) {
            System.out.println("Loading default configuration from classpath: default.properties");
            try {
                properties.load(defaultConfigStream);
            } catch (IOException e) {
                System.err.println("Error reading default config: " + e.getMessage());
            }
        }
        // Addtional default in working directory
        File defaultConfig = new File("eu/dm2e/direct/default.properties");
        if (defaultConfig.exists()) {
            System.out.println("Loading default configuration from workdir: default.properties");
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

        if (cmd.getArgs().length == 0 && properties.getProperty("input") == null && !(cmd.hasOption("delete-version") || cmd.hasOption("delete-all-versions"))) {
            System.out.println("No input files. Either add them to a config or pass as argument: ");
            HelpFormatter help = new HelpFormatter();
            help.printHelp("ingest", options);
            return;
        }

        System.out.println("Configuration used: ");
        properties.list(System.out);

        if (cmd.hasOption("delete-version") || cmd.hasOption("delete-all-versions")) {
            new Ingestion().delete(cmd, properties);
            return;
        }

        new Ingestion().ingest(cmd, properties);

    }

    public void delete(CommandLine cmd, Properties properties) {
        endpointUpdate = properties.getProperty("endpointUpdate");
        endpointSelect = properties.getProperty("endpointSelect");
        System.out.println("DELETE MODE!!!");
        if (cmd.hasOption("delete-version"))   {
            System.out.println("The following graph will be deleted: " + cmd.getOptionValue("delete-version"));
            if (areYouSure()) {
                deleteVersion(cmd.getOptionValue("delete-version"));
            }
            return;
        }
        if (cmd.hasOption("delete-all-versions"))   {
            String collection =  cmd.getOptionValue("delete-all-versions");
            ResultSet iter = new SparqlSelect.Builder()
                    .where(String.format("graph ?g {?g <http://www.w3.org/ns/prov#specializationOf> <%s> }", collection))
                    .select("?g")
                    .endpoint(endpointSelect)
                    .build()
                    .execute();
            List<String> versions = new ArrayList<>();
            System.out.println("The following versions will be deleted:");
            while (iter.hasNext()) {
                String version = iter.next().get("?g").asResource().getURI();
                versions.add(version);
                System.out.println("   " + version);
            }
            if (areYouSure()) {
                for (String s:versions) {
                    deleteVersion(s);
                }
            }

        }
    }

    private boolean areYouSure() {
        System.out.println("Are your sure (yes/no)?");
        try {
            String answer = new BufferedReader(new InputStreamReader(System.in)).readLine();
            if (answer.equals("yes")) return true;
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
        return false;
    }

    private void deleteVersion(String version) {
        Grafeo g = new GrafeoImpl();
        g.emptyGraph(endpointUpdate, version);
        System.out.println("Graph <" + version + "> deleted.");
    }

    public void ingest(CommandLine cmd, Properties properties) {
        long start = System.currentTimeMillis();

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
        if (properties.get("validation") != null) {
            String value = properties.get("validation").toString();
            if (value.equals("ERROR")) validationLevel = ValidationLevel.ERROR;
            if (value.equals("FATAL")) validationLevel = ValidationLevel.FATAL;
            if (value.equals("WARNING")) validationLevel = ValidationLevel.WARNING;
            if (value.equals("NOTICE")) validationLevel = ValidationLevel.WARNING;
            if (value.equals("OFF")) validationLevel = null;
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
            ingestionLogFile = new File("ingestion-" + provider + "-" + dataset + "-" + version + ".log");
            ingestionLog = new FileWriter(ingestionLogFile, true);
            ingestionLog.write("New log file\n");
            log("Check logging: ok");
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

        System.out.println("Data will be ingested to dataset: " + graphName);

        if (properties.get("dm2e-model-version") == null) {
            final String msg = "dm2e-model-version is not set!";
            log(msg, System.err);
            log("Supported versions:", System.err);
            for (Dm2eSpecificationVersion thisVersion : Dm2eSpecificationVersion.values()) {
                log("  * " + thisVersion.getVersionString(), System.err);
            }
            dm2eModelVersion = properties.get("dm2e-model-version").toString();
            return;
        }

        final String dm2eModelVersion = properties.getProperty("dm2e-model-version");
        if (validationLevel != null) {
            try {
                validator = Dm2eSpecificationVersion.forString(dm2eModelVersion).getValidator();
            } catch (NoSuchFieldException e1) {
                final String msg = "Unsupported 'dm2e-model-version': " + dm2eModelVersion;
                log(msg, System.err);
                log("Supported versions:", System.err);
                for (Dm2eSpecificationVersion thisVersion : Dm2eSpecificationVersion.values()) {
                    log("  * " + thisVersion.getVersionString(), System.err);
                }
                return;
            }
        }

        if (xslt != null) {
            System.out.println("XSLT Stylesheet: " + xslt);
            try {
                xslLogFile = new File("xsl-" + provider + "-" + dataset + "-" + version + ".log");
                xslLog = new FileWriter(xslLogFile, true);
            } catch (IOException e) {
                throw new RuntimeException("An exception occurred: " + e, e);
            }
            System.out.println("See xsl.log for messages from the XSLT process.");

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

    /**
     * Logs to the XSL transformation log
     *
     * @param message
     */
    protected void xslLog(String message) {
        try {
            xslLog.write(message);
            xslLog.write("\n");
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

    }

    /**
     * Logs to the ingestion log
     *
     * @param message
     */
    protected void log(String message) {
        try {
            ingestionLog.write(new java.text.SimpleDateFormat("MM/dd hh:mm:ss").format(new Date()) + ": " + message);
            ingestionLog.write("\n");
            ingestionLog.flush();
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

    }

    /**
     * Logs to the ingestion log and the given print stream (such aus System.out or System.err)
     *
     * @param message the message
     * @param out     the print stream to write to
     */
    protected void log(String message, PrintStream out) {
        log(message);
        out.println(message);
    }

    /**
     * Logs to the ingestion log
     *
     * @param t
     */
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


    public void processFile(String input) throws ValidationException {
        try {
            log("===============================");
            log("Date: " + new Date());
            log("File: " + input);

            File tmp = new File(input);
            if (xslt != null) {
                xslLog("===============================");
                xslLog("Date: " + new Date());
                xslLog("File: " + input);
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
//            GrafeoImpl g = new GrafeoImpl(tmp);
            Model jenaModel = ModelFactory.createDefaultModel();
            jenaModel.read(new FileInputStream(tmp), null);
            //
            // Validation!
            //
            if (validationLevel != null) {
                Dm2eValidationReport validationReport = validator.validateWithDm2e(jenaModel);
                if (!validationReport.containsErrors(validationLevel)) {
                	log("Output validated. Yay :)");
                	postToEndpoint(jenaModel);
                } else {
                    log(validationReport.exportToString(validationLevel, true, false));
                    throw new ValidationException(validationReport);
                }
            } else {
                postToEndpoint(jenaModel);
            }
            System.out.print(".");
            fileCount++;
            if (fileCount % 50 == 0) {
                System.out.println("   " + fileCount);
            }
        } catch (ValidationException e) {
            System.out.print("v");
            fileCount++;
            if (fileCount % 50 == 0) {
                System.out.println("   " + fileCount);
            }
            throw e;
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

	private void postToEndpoint(Model jenaModel) {
		StringWriter sw = new StringWriter();
		RDFWriter rdfWriter = jenaModel.getWriter("N-TRIPLE");
		rdfWriter.write(jenaModel, sw, null);
		ParameterizedSparqlString sb = new ParameterizedSparqlString();
		sb.append("INSERT {  \n");
		sb.append("  GRAPH < " + graphName + "> {  \n");
		sb.append(		sw.toString());
		sb.append("  }  \n");
		sb.append("}  \n");
		sb.append("WHERE { } ");
		UpdateRequest update = UpdateFactory.create();
		update.add(sb.toString());
		log.info("UPDATE request {}", update.toString());
		log("UPDATE Request: " + update.toString());
		UpdateProcessor exec = UpdateExecutionFactory.createRemoteForm(update, endpointUpdate);
		exec.execute();
	}


    /**
     * Finds the root stylesheet in a directory.
     *
     * @param zipdir Directory containing the unzipped xslt files
     * @return the first stylesheet containing 'xsl:template match="/"'
     */
    public String grepRootStylesheet(String zipdir) {
        if (!new File(zipdir).isDirectory()) return zipdir;
        Pattern pattern = Pattern.compile("xsl:template match=\"/\"");
        String rootFile = null;
        for (String file : DataTool.getFiles(zipdir)) {

            Scanner scanner = null;
            try {
                scanner = new Scanner(new File(file));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("An exception occurred: " + e, e);
            } finally {
                scanner.close();
            }
            if (scanner.findWithinHorizon(pattern, 0) != null) {
                rootFile = file;
                break;
            }
        }

        return rootFile;

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
        activity.setStart(new DateTime());

        VersionedDatasetPojo ds = new VersionedDatasetPojo();
        ds.setId(graphName);
        ds.setLabel(label != null ? label : dataset);
        ds.setComment("XSLT: " + originalXslt + " Input: " + input + " Generated: " + new Date() + " by DM2E Direct Ingestion.");
        ds.setTimestamp(DateTime.now());
        ds.setDatasetID(URI.create(datasetURI));
        ds.findLatest(endpointSelect);
        ds.setJobURI(URI.create(activity.getId()));
        ds.setDm2eModelVersion(dm2eModelVersion);
        ds.setValidatedAtLevel(validationLevel == null ? "OFF" : validationLevel.name());


        List<String> errors = new ArrayList<String>();
        ValidationException validationException = null;
        if (!useOAIPMH) {
            File f = new File(input);
            if (f.isFile()) {
                try {
                    processFile(input);
                } catch (ValidationException e) {
                    validationException = e;
                    errors.add(input);
                } catch (RuntimeException t) {
                    log(t);
                    log.error("\n Ingestion Error: " + t.getMessage(), t);
                    errors.add(input);
                }
            } else if (f.isDirectory()) {
                for (String file : DataTool.getFiles(f.toString())) {
                    try {
                        processFile(file);
                    } catch (ValidationException e) {
                        validationException = e;
                        errors.add(file);
                        break;
                    } catch (RuntimeException t) {
                        log(t);
                        log.error("\n Ingestion Error: " + t.getMessage(), t);
                        errors.add(file);
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
                } catch (ValidationException e) {
                    validationException = e;
                    errors.add(id);
                    break;
                } catch (RuntimeException t) {
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
        if (null == validationException) {
            activity.setEnd(new DateTime());
            Grafeo g = new GrafeoImpl();
            g.getObjectMapper().addObject(ds);
            g.getObjectMapper().addObject(activity);
            g.postToEndpoint(endpointUpdate, graphName);
            log.info("Provenance: " + g.getTerseTurtle());
            System.out.println("Provenance written.");
            System.out.println("Check result at: http://data.dm2e.eu/data/html/dataset/" + provider + "/" + dataset + "/" + version);
        } else {
            log("Validation Errrors occured, NOT INGESTED.", System.out);
            // Not again...
            // log(validationException.getReport().exportToString(ValidationLevel.ERROR, true, true));
        }
        System.out.println("Report: " + ingestionLogFile.getName());
        if (xslLogFile != null) System.out.println("XSLT Report: " + xslLogFile.getName());

    }

    /**
     * Finds the actual location of an input
     * <p>
     * If the input is a directory name it is returned
     * </p>
     * <p>
     * If the input is an HTTP or an FTP link, it is downloaded and the local location returned
     * <p>
     * If the input is the path to a ZIP file, it is unzipped and the unzip
     * directory is returned
     * </p>
     *
     * @param input the input locator as a String
     * @return the actual input locator
     * @throws {@link RuntimeException} if input is a ZIP and unzipping fails {@link net.lingala.zip4j.exception.ZipException}
     */
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
