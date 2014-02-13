package eu.dm2e.direct;

import javax.xml.namespace.NamespaceContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This file was created within the DM2E project.
 * http://dm2e.eu
 * http://github.com/dm2e
 * <p/>
 * Author: Kai Eckert, Konstantin Baierer
 */
public class Namespaces implements NamespaceContext {
    Map<String,String> namespaces = new HashMap<String, String>();
    Map<String,String> reverse = new HashMap<String, String>();

    public void addNamespace(String prefix, String namespace) {
        namespaces.put(prefix,namespace);
        reverse.put(namespace, prefix);
    }

    @Override
    public String getNamespaceURI(String s) {
        return namespaces.get(s);
    }

    @Override
    public String getPrefix(String s) {
        return reverse.get(s);
    }

    @Override
    public Iterator<String> getPrefixes(String s) {
        return namespaces.keySet().iterator();
    }
}
