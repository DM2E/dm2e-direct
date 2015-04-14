# DM2E Direct #

dm2e-ingest is a wrapper script around the jar file, which otherwise can also be executed directly via java -jar JARFILE.

If you call dm2e-ingest, you get the following usage information:

<pre>
dm2e-ingest 
Loading default configuration from classpath: default.properties
No input files. Either add them to a config or pass as argument: 
usage: ingest
 -c,--config <arg>                   The direct configuration to be used.
 -d,--dataset <arg>                  The dataset ID this is used to create
                                     your URIs, e.g., codices.
 -delA,--delete-all-versions <arg>   Collection URI. Delete all ingestions
                                     for this collection (USE WITH
                                     CARE!!!!)
 -delV,--delete-version <arg>        Graph URI. Delete this ingestion (USE
                                     WITH CARE!)
 -dm2e,--dm2e-model-version <arg>    DM2E Model Version. Leave empty to
                                     get a list of supported versions.
 -e,--exclude <arg>                  comma-separated list of identifiers
                                     or files to be excluded
 -h,--help                           Show this help.
 -i,--include <arg>                  comma-separated list of identifiers
                                     or files to be included
 -kt,--keep-temporary <arg>          Keep temporary files (in default temp
                                     directory) for debugging.
 -l,--label <arg>                    A label describing the ingested
                                     dataset.
 -p,--provider <arg>                 The provider ID that is used to
                                     create your URIs, e.g., mpiwg.
 -pmh,--use-oai-pmh <arg>            Set to true, if input is an OAI-PMH
                                     endpoint.
 -v,--validation <arg>               Validate and stop at specified level.
                                     One of FATAL, ERROR, WARNING, NOTICE,
                                     OFF. Default: ERROR
 -x,--xslt <arg>                     The XSLT mapping (zipped, a folder or
                                     a file)
 -xp,--xslt-props <arg>              The file containing properties passed
                                     to the XSLT process.

</pre>

You can provide all arguments in a configuration file for easier provenance tracking and repetition. Check the examples for further information.