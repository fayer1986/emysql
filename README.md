
# emysql

Erlang MySQL client.

## Compile with Rebar

* rebar compile

## API

### Select

* emysql:select(tab).
* emysql:select({tab, [col1,col2]}).
* emysql:select({tab, [col1, col2], {id,1}}).
* emysql:select(Query, Load).

### Update

* emysql:update(tab, [{Field1, Val}, {Field2, Val2}], {id, 1}).

### Insert

* emysql:insert(tab, [{Field1, Val}, {Field2, Val2}]).

### Delete

* emysql:delete(tab, {name, Name}]).

### Raw Query

* emysql:sqlquery("select * from tab;").

### Prepare

* emysql:prepare(find_with_id, "select * from tab where id = ?;").
* emysql:execute(find_with_id, [Id]).
* emysql:unprepare(find_with_id).

## MySQL Client Protocal

* http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

## Author

Feng Lee <feng@emqtt.io>

