package eu.dm2e.direct;

import eu.dm2e.NS;
import eu.dm2e.grafeo.annotations.RDFClass;
import eu.dm2e.grafeo.annotations.RDFProperty;
import eu.dm2e.grafeo.gom.SerializablePojo;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * This file was created within the DM2E project.
 * http://dm2e.eu
 * http://github.com/dm2e
 * <p/>
 * Author: Kai Eckert, Konstantin Baierer
 */

@RDFClass(NS.PROV.CLASS_ACTIVITY)
public class IngestionActivity extends SerializablePojo<IngestionActivity> {

    @RDFProperty(NS.PROV.PROP_USED)
    private Set<URI> inputs = new HashSet<URI>();

    @RDFProperty(NS.PROV.PROP_USED)
    private URI xslt;

    @RDFProperty(NS.PROV.PROP_WAS_ASSOCIATED_WITH)
    private URI agent;

    public Set<URI> getInputs() {
        return inputs;
    }

    public void setInputs(Set<URI> inputs) {
        this.inputs = inputs;
    }

    public URI getXslt() {
        return xslt;
    }

    public void setXslt(URI xslt) {
        this.xslt = xslt;
    }

    public URI getAgent() {
        return agent;
    }

    public void setAgent(URI agent) {
        this.agent = agent;
    }

    @RDFProperty(NS.PROV.PROP_WAS_STARTED_AT)
    private DateTime start;
    public DateTime getStart() { return start; }
    public void setStart(DateTime start) { this.start = start; }

    @RDFProperty(NS.PROV.PROP_WAS_ENDED_AT)
    private DateTime end;
    public DateTime getEnd() { return end; }
    public void setEnd(DateTime end) { this.end = end; }
}
