cql_schema_versioning
=====================

This is an example that I threw together very quickly, so beware. It's bare-bones, because I only put in what I needed for my particular purposes. It stores the list of scripts it's run in a table/cf named 'version'. It depends on the sort order of the script names to determine run order. 

Build using: mvn clean install

Then see cql-schema-versioning-example to see how it can be used.
