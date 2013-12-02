/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.dm2e.direct;

import eu.dm2e.grafeo.Grafeo;
import eu.dm2e.grafeo.jena.GrafeoImpl;
import net.lingala.zip4j.core.ZipFile;
import net.sf.saxon.serialize.MessageEmitter;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A direct ingestion tool to ingest XML data via an XSLT mapping
 * into a triple store.
 *
 *
 * @author Dominique Ritze, Kai Eckert
 */
public class Ingestion {


    String xslt;
    String endpointUpdate;
    String endpointSelect;
    String provider;
    String dataset;
    String base;
    String graphName;
    FileWriter xslLog;
    long fileCount = 0;



    public static void main(String args[]) {



        Options options = new Options();
        options.addOption("c","config",true,"The direct configuration to be used.");
        options.addOption("p","provider",true,"The provider ID that is used to create your URIs, e.g., mpiwg.");
        options.addOption("d","dataset",true,"The dataset ID this is used to create your URIs, e.g., codices.");
        options.addOption("l","label", true, "A label describing the ingested dataset.");
        options.addOption("x","xslt", true, "The XSLT mapping (zipped, a folder or a file)");
        options.addOption("h","help", false, "Show this help.");

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
                properties.load(new FileInputStream( cmd.getOptionValue("c")));
            } catch (IOException e) {
                System.err.println("Error reading custom config: " + e.getMessage());
            }
        }

        for (Option o:cmd.getOptions()) {
            properties.put(o.getLongOpt(),o.getValue());
        }

        if (cmd.getArgs().length==0 && properties.getProperty("input")==null) {
            System.out.println("No input files. Either add them to a config or pass as argument: ");
            HelpFormatter help = new HelpFormatter();
            help.printHelp("ingest", options);
            return;
        }

        System.out.println("Configuration used: ");
        properties.list(System.out);

        new Ingestion().ingest(cmd,properties);

    }

    public void ingest(CommandLine cmd, Properties properties) {
        long start = System.currentTimeMillis();
        xslt = properties.getProperty("xslt");
        endpointUpdate = properties.getProperty("endpointUpdate");
        endpointSelect = properties.getProperty("endpointSelect");
        provider = properties.getProperty("provider");
        dataset = properties.getProperty("dataset");
        base = properties.getProperty("base");
        graphName = base + provider.toLowerCase() + "/" + dataset.toLowerCase() + "/" + DateTime.now().getMillis();
        try {
            xslLog = new FileWriter(new File("xsl.log"), true);
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

        System.out.println("Data will be ingested to dataset: " + graphName);

        xslt = prepareInput(xslt);
        xslt = grepRootStylesheet(xslt);

        long setupTime = System.currentTimeMillis() - start;
        start = System.currentTimeMillis();
        System.out.println("Time for XSLT setup in sec: " + ((double)setupTime) / 1000);

        if (cmd.getArgs().length==0) {
            String input = properties.getProperty("input");
            input = prepareInput(input);
            long args = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            System.out.println("Time for default data setup in sec: " + ((double)args) / 1000);
            processFileOrFolder(input);
        }


        for (String input:cmd.getArgs()) {
            long args = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            System.out.println("Time for data setup in sec: " + ((double)args) / 1000);
            input = prepareInput(input);
            processFileOrFolder(input);
        }
        long end = System.currentTimeMillis() - start;
        System.out.println("\nTime for transformation in sec: " + ((double)end) / 1000);



    }

    protected void xslLog(String message) {
        try {
            xslLog.write(message);
            xslLog.write("\n");
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }

    }

    public void processFile(String input) {
        xslLog("===============================");
        xslLog("Date: " + new Date());
        xslLog("File: " + input);
        File f = new File(input);

        StreamSource xslSource = new StreamSource(xslt);
        xslSource.setSystemId(xslt);

        StreamSource xmlSource = new StreamSource(f);
        xmlSource.setSystemId(f);

        File tmp = null;
        try {
            tmp = File.createTempFile("TRANS_", ".xml");
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
        //tmp.deleteOnExit();
        FileWriter resultXML = null;
        try {
            resultXML = new FileWriter(tmp);
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
        TransformerFactory transFact = new net.sf.saxon.TransformerFactoryImpl();

        try {
            Transformer trans = transFact.newTransformer(
                    new StreamSource(xslt));
            MessageEmitter emitter = new MessageEmitter();
            emitter.setWriter(xslLog);
            ((net.sf.saxon.Controller)trans).setMessageEmitter(emitter);
            trans.transform(new StreamSource(f),
                    new StreamResult(resultXML));
        } catch (TransformerException e) {
            System.out.println("XSLT: " + xslt);
            System.out.println("File: " + f.toString());

            throw new RuntimeException("An exception occurred: " + e, e);
        }
        try {
            resultXML.flush();
            resultXML.close();
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
        Grafeo g = new GrafeoImpl(tmp);
        g.postToEndpoint(endpointUpdate, graphName);
        System.out.print(".");
        fileCount++;
        if (fileCount%50==0) {
            System.out.println("   " + fileCount);
        }


    }

    public List<String> getFiles(String dir) {
        List<String> res = new ArrayList<String>();
        File d = new File(dir);
        if (d.isDirectory()) {
            for (File f:d.listFiles()) {
                if (f.isDirectory()) res.addAll(getFiles(f.toString()));
                else {res.add(f.toString());}
            }

        } else {
            res.add(d.toString());
        }
        return res;
    }

    public String grepRootStylesheet(String zipdir) {
        Pattern pattern = Pattern.compile("xsl:template match=\"/\"");
        for (String file:getFiles(zipdir)) {

            Scanner scanner = null;
            try {
                scanner = new Scanner(new File(file));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("An exception occurred: " + e, e);
            }
            if (scanner.findWithinHorizon(pattern,0)!=null) return file;
        }

        return null;

    }

    public void processFileOrFolder(String input) {
        File f = new File(input);
        if (f.isFile()) {
            processFile(input);
            return;
        }
        if (f.isDirectory()) {
        for (String file : getFiles(f.toString())) {
            processFile(file);
        }

        }

        Grafeo g = new GrafeoImpl();
        g.addTriple(graphName, "rdfs:comment", "XSLT: " + xslt + " Input: " + input + " Generated: " + new Date() + " by Kai");
        g.postToEndpoint(endpointUpdate, graphName);




    }

    public String prepareInput(String input) {
        if (new File(input).isDirectory()) return input;
        if (input.startsWith("http") || input.startsWith("ftp")) {
            input = download(input);
            try {
                if (new ZipFile(input).isValidZipFile()) {
                    input = unzip(input);
                }
            } catch (net.lingala.zip4j.exception.ZipException e) {
                throw new RuntimeException("An exception occurred: " + e, e);
            }
        }
        return input;
    }

    public String download(String input) {
        Client client = ClientBuilder.newClient();
        Response resp = client.target(input).request().get();
        if (resp.getStatus() >= 400) {
            System.err.println("Could not download: " + resp.readEntity(String.class));
            return null;
        }
        java.nio.file.Path inputPath;
        FileOutputStream input_fos;
        try {
            inputPath = Files.createTempFile("download_", ".tmp");
        } catch (IOException e1) {
            System.err.println("Could not create temp file: " + e1.getMessage());
            return null;
        }

        try {
            input_fos = new FileOutputStream(inputPath.toFile());
        } catch (FileNotFoundException e1) {
            System.err.println("Could not write to temp file: " + e1.getMessage());
            return null;
        }
        try {
            IOUtils.copy(resp.readEntity(InputStream.class), input_fos);
        } catch (IOException e) {
            System.err.println("Could not write to temp file: " + e.getMessage());
            return null;
        } finally {
            IOUtils.closeQuietly(input_fos);
        }
        inputPath.toFile().deleteOnExit();
        return inputPath.toString();
    }

    public String unzip(String input) {
        java.nio.file.Path zipdir;
        try {
            zipdir = Files.createTempDirectory("unzip_");
        } catch (IOException e) {
            System.err.println("Could not write to temp dir: " + e.getMessage());
            return null;
        }
        try {
            ZipFile zipFile = new ZipFile(new File(input));
            zipFile.extractAll(zipdir.toString());
        } catch (net.lingala.zip4j.exception.ZipException e) {
            System.err.println("Error during unzipping: " + e.getMessage());
            return null;
        }
        zipdir.toFile().deleteOnExit();
        return zipdir.toString();
    }
}
