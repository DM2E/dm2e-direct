package eu.dm2e.direct;

import net.lingala.zip4j.core.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * This file was created within the DM2E project.
 * http://dm2e.eu
 * http://github.com/dm2e
 * <p/>
 * Author: Kai Eckert, Konstantin Baierer
 */
public class DataTool {


    public static String download(String input) {
        URI website = URI.create(input);
        ReadableByteChannel rbc = null;
        try {
            rbc = Channels.newChannel(website.toURL().openStream());
            Path inputPath;
            FileOutputStream fos;
            inputPath = Files.createTempFile("download_", ".tmp");
            fos = new FileOutputStream(inputPath.toFile());
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.flush();
            fos.close();
            inputPath.toFile().deleteOnExit();
            return inputPath.toString();
        } catch (IOException e) {
            System.err.println("Could not download file: " + e.getMessage());
            return null;
        }
    }

    public static String unzip(String input) {
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

    public static List<String> getFiles(String dir) {
        List<String> res = new ArrayList<String>();
        File d = new File(dir);
        if (d.isDirectory()) {
            for (File f : d.listFiles()) {
                if (f.isDirectory()) res.addAll(getFiles(f.toString()));
                else {
                    res.add(f.toString());
                }
            }

        } else {
            res.add(d.toString());
        }
        return res;
    }

    public static Document getXMLDocument(String file) {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = null;
        try {
            docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse (new File(file));
            return doc;
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        } catch (SAXException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        } catch (IOException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
    }

    public static List<String> getXPathStrings(Document input, String expression) {
        XPath xPath =  XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(NS);
        try {
            NodeList nodes = (NodeList) xPath.compile(expression).evaluate(input, XPathConstants.NODESET);
            List<String> res = new ArrayList<String>();
            for (int i = 0; i < nodes.getLength();i++) {
                res.add(nodes.item(i).getNodeValue());
            }
            return res;
        } catch (XPathExpressionException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
    }

    public static final Namespaces NS = new Namespaces();
    static {
        NS.addNamespace("oai", "http://www.openarchives.org/OAI/2.0/");
        NS.addNamespace("cmdi", "http://www.clarin.eu/cmd/");
    }

    public static String getXPathString(Document input, String expression) {
        XPath xPath =  XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(NS);
        try {
            return xPath.compile(expression).evaluate(input);
        } catch (XPathExpressionException e) {
            throw new RuntimeException("An exception occurred: " + e, e);
        }
    }


}
