create table if not exists table_10K_numeric(
       rowid int not null,
       col1 int,
       col2 float,
       col3 float,
       col4 float,
       col5 float,
       col6 float,
       col7 float)
order by rowid
segmented by modularhash(rowid) all nodes;


create table if not exists table_10K(
       rowid int not null,
       col1 int,
       col2 float,
       col3 float,
       col4 float,
       col5 float,
       col6 float,
       col7 float,
       col8 char(1) encoding rle,
       col9 char(1) encoding rle,
       col10 char(1) encoding rle)
order by rowid
segmented by modularhash(rowid) all nodes;

create table if not exists table_graph(
       u int not null,
       v int not null,
       weight float)
order by v
segmented by modularhash(v) all nodes;

