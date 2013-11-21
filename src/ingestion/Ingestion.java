/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ingestion;

import eu.dm2e.grafeo.Grafeo;
import eu.dm2e.grafeo.jena.GrafeoImpl;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.lingala.zip4j.core.ZipFile;

/**
 *
 * @author domi
 */
public class Ingestion {

    static File folder;
    static File xslt;
    static File archive;
    static boolean temporary = false;
    static String endpoint;
    static String graphName;

    public static void main(String args[]) throws IOException, ZipException, net.lingala.zip4j.exception.ZipException, TransformerConfigurationException, TransformerException {

        Properties properties = new Properties();
        BufferedInputStream stream;

        if (args.length > 0) {
            stream = new BufferedInputStream(new FileInputStream(new File(args[0])));
        } else {
            stream = new BufferedInputStream(new FileInputStream("ingestion.properties"));
        }
        properties.load(stream);
        stream.close();
        if (properties.getProperty("XMLFolder").isEmpty()) {
            temporary = true;
            archive = new File(properties.getProperty("ARCHIVE"));
        } else {
            folder = new File(properties.getProperty("XMLFolder"));
        }
        xslt = new File(properties.getProperty("XSLT"));
        endpoint = properties.getProperty("SPARQL_ENDPOINT");
        graphName = properties.getProperty("GRAPH_NAME");

        if (temporary) {
            java.nio.file.Path zipdir = Files.createTempDirectory("omnom_archive_");
            ZipFile zipFile = new ZipFile(archive);
            zipFile.extractAll(zipdir.toString());


            folder = new File(zipdir.toString());
            folder.deleteOnExit();
        }
        System.out.println("fold: " +folder);
        for (File f : folder.listFiles()) {
            System.out.println(f.getAbsolutePath());

            StreamSource xslSource = new StreamSource(xslt);
            xslSource.setSystemId(xslt);

            StreamSource xmlSource = new StreamSource(f);
            xmlSource.setSystemId(f);

            File tmp = File.createTempFile("TRANS_", ".xml");
            //tmp.deleteOnExit();
            System.out.println(tmp.getAbsolutePath());
            FileWriter resultXML = new FileWriter(tmp);
            TransformerFactory transFact = new net.sf.saxon.TransformerFactoryImpl();
            transFact.newTransformer(
                    new StreamSource(xslt)).transform(new StreamSource(f),
                    new StreamResult(resultXML));
            resultXML.flush();
            resultXML.close();
            Grafeo g = new GrafeoImpl(tmp);
            g.postToEndpoint(endpoint, graphName);
            System.out.print(".");

        }


        Long start = System.currentTimeMillis();

        //transform(xslt, folder, );
        //writeToTS(outputFolder, "http://lelystad.informatik.uni-mannheim.de:3040/ds/update","http://data.dm2e.eu/data/dataset/onb/codices/1");

        Grafeo g = new GrafeoImpl();
        g.addTriple(graphName, "rdfs:comment", "XSLT: " + xslt + " XML-Folder: " + folder + " Archive: " + archive + " Generated: " + new Date() + " by Kai");
        g.postToEndpoint(endpoint, graphName);

        long time = System.currentTimeMillis() - start;

        System.out.println("\n time:" + time);



    }
}
