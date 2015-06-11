\set pwd `pwd`

\set datafile '\'':pwd'/../data/table_1k.dat\''
COPY table_1K from :datafile delimiter '|' DIRECT;

\set datafile '\'':pwd'/../data/table_10k.dat\''
COPY table_10K from :datafile delimiter '|' DIRECT;

\set datafile '\'':pwd'/../data/table_100k.dat\''
COPY table_100K from :datafile delimiter '|' DIRECT;

\set datafile '\'':pwd'/../data/graph3.dat\''
COPY table_graph from :datafile delimiter '|' DIRECT;
