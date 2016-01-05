
%%------------------------------------------------------------------------------
%% MySQL Versions
%%------------------------------------------------------------------------------

-define(MYSQL_4_0, 40). %% for MySQL 4.0.x
-define(MYSQL_4_1, 41). %% for MySQL 4.1.x, 5.x, 6.x

%%------------------------------------------------------------------------------
%% MySQL OP
%%------------------------------------------------------------------------------

-define(MYSQL_QUERY_OP, 3).

%%------------------------------------------------------------------------------
%% MySQL Datatypes
%%------------------------------------------------------------------------------

-define(DECIMAL,     0).
-define(TINY,        1).
-define(SHORT,       2).
-define(LONG,        3).
-define(FLOAT,       4).
-define(DOUBLE,      5).
-define(NULL,        6).
-define(TIMESTAMP,   7).
-define(LONGLONG,    8).
-define(INT24,       9).
-define(DATE,        10).
-define(TIME,        11).
-define(DATETIME,    12).
-define(YEAR,        13).
-define(NEWDATE,     14).
-define(NEWDECIMAL,  246).
-define(ENUM,        247).
-define(SET,         248).
-define(TINYBLOB,    249).
-define(MEDIUM_BLOG, 250).
-define(LONG_BLOG,   251).
-define(BLOB,        252).
-define(VAR_STRING,  253).
-define(STRING,      254).
-define(GEOMETRY,    255).

-define(IS_INTEGER(T), (T =:= 'TINY'     orelse
                        T =:= 'SHORT'    orelse
                        T =:= 'LONG'     orelse
                        T =:= 'LONGLONG' orelse
                        T =:= 'INT24'    orelse
                        T =:= 'YEAR')).

-define(IS_FLOAT(T), (T =:= 'DECIMAL'    orelse
                      T == 'NEWDECIMAL'  orelse
                      T == 'FLOAT'       orelse
                      T == 'DOUBLE')).

-define(IS_DATETIME(T), (T =:= 'TIMESTAMP' orelse
                         T =:= 'DATETIME')).

%%------------------------------------------------------------------------------
%% MySQL Packet
%%------------------------------------------------------------------------------

-record(mysql_greeting, {seqnum, protocol, version, thread,
                         salt1, caps, server, salt2}).

-type mysql_greeting() :: #mysql_greeting{}.

