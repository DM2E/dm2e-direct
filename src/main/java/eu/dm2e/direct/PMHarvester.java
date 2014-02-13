package eu.dm2e.direct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * This file was created within the DM2E project.
 * http://dm2e.eu
 * http://github.com/dm2e
 * <p/>
 * Author: Kai Eckert, Konstantin Baierer
 */
public class PMHarvester {

    private String endpoint;
//    private String getRecord;
    protected Logger log = LoggerFactory.getLogger(getClass().getName());

    public PMHarvester(String endpoint) {
        this.endpoint = endpoint;
//        this.getRecord = endpoint + "?verb=GetRecord&metadataPrefix=cmdi&identifier=";

    }

    protected List<String> getIdentifiers(String resumptionToken) {
        List<String> res = new ArrayList<String>();
        String uri =  endpoint + "?verb=ListIdentifiers&resumptionToken=" + resumptionToken;
        log.info("Requesting " + uri);
        String file = DataTool.download(uri);
        Document doc = DataTool.getXMLDocument(file);
        res.addAll(DataTool.getXPathStrings(doc, "//oai:identifier/text()"));
        String newToken = DataTool.getXPathString(doc, "/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken/text()");
        if (newToken!=null && !newToken.isEmpty()) res.addAll(getIdentifiers(newToken));
        return res;
    }

    public List<String> getIdentifiers() {
        List<String> res = new ArrayList<String>();
        String uri =  endpoint + "?verb=ListIdentifiers&set=dta&metadataPrefix=oai_dc";
        log.info("Requesting " + uri);
        String file = DataTool.download(uri);
        Document doc = DataTool.getXMLDocument(file);
        res.addAll(DataTool.getXPathStrings(doc, "//oai:identifier/text()"));
        String newToken = DataTool.getXPathString(doc, "/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken/text()");
        if (newToken!=null && !newToken.isEmpty()) res.addAll(getIdentifiers(newToken));
        return res;
    }

    public String getRecord(String identifer) {
        String uri =  endpoint + "?verb=GetRecord&metadataPrefix=cmdi&identifier=" + identifer;
        log.info("Requesting " + uri);
        String file = DataTool.download(uri);
        Document doc = DataTool.getXMLDocument(file);
        return DataTool.getXPathString(doc, "//cmdi:ResourceProxy[./cmdi:ResourceType/@mimetype eq 'application/xml']/cmdi:ResourceRef/text()");
    }


    public static void main(String args[]) {
        DataTool.NS.addNamespace("oai", "http://www.openarchives.org/OAI/2.0/");
        DataTool.NS.addNamespace("cmdi", "http://www.clarin.eu/cmd/");
        PMHarvester harvester = new PMHarvester("http://fedora.dwds.de/oai-dta/");
        List<String> ids = harvester.getIdentifiers();
        for (String id:ids) {
            System.out.println("ID: " + id + " -> " + harvester.getRecord(id));
        }
        System.out.println("Found: " + ids.size());
    }


}
