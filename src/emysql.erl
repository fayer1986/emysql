%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2015-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc MySQL Driver API Module.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------

-module(emysql).

-include("emysql.hrl").

-export([connect/1, query/2, query/3, prepare/3, execute/3, unprepare/2,
         info/1, close/1]).

-export([escape/1, escape_like/1]).

-type client() :: pid().

-type sql() :: string() | iodata().

-type result() :: {ok, updated, {}} | {ok, data, {list(), list()}} | {error, any()}.

%% @doc Connect to MySQL Database.
-spec connect([emysql_client:option()]) -> {ok, client()} | {error, any()}.
connect(Opts) -> emysql_client:connect(Opts).

%% @doc SQL Query
-spec query(client(), sql()) -> result().
query(Client, SQL) ->
    result(emysql_client:query(Client, iolist_to_binary(SQL))).

-spec query(client(), sql(), timeout()) -> result().
query(Client, SQL, Timeout) ->
    result(emysql_client:query(Client, iolist_to_binary(SQL), Timeout)).

%% @doc Prepare
-spec prepare(client(), sql(), sql()) -> {ok, result()} | {error, any()}.
prepare(Client, Name, Stmt) ->
    result(emysql_client:prepare(Client, iolist_to_binary(Name), iolist_to_binary(Stmt))).

%% @doc Execute Statement
-spec execute(client(), sql(), list()) -> {ok, result()} | {error, any()}.
execute(Client, Name, Params) ->
    emysql_client:execute(Client, iolist_to_binary(Name), Params).

%% @doc Unprepare
-spec unprepare(client(), sql()) -> {ok, result()} | {error, any()}.
unprepare(Client, Name) ->
    emysql_client:unprepare(Client, iolist_to_binary(Name)).

%% @doc Info
info(Client) -> emysql_client:info(Client).

%% @doc Close the connection.
-spec close(client()) -> ok.
close(Client) -> emysql_client:close(Client).

%% @doc Escape character that will confuse an SQL engine
%% Percent and underscore only need to be escaped for pattern matching like
%% statement
%% @end
escape_like(Str) when is_binary(Str) ->
    << <<(escape_like(C))/binary>> || <<C>> <= Str >>;
escape_like($%) -> <<"\\%">>;
escape_like($_) -> <<"\\_">>;
escape_like(C)  -> escape(C).

%% @doc Escape character that will confuse MySQL.
escape(Str) when is_binary(Str) ->
    << <<(escape(C))/binary>> || <<C>> <= Str >>;
escape($\0) -> <<"\\0">>;
escape($\n) -> <<"\\n">>;
escape($\t) -> <<"\\t">>;
escape($\b) -> <<"\\b">>;
escape($\r) -> <<"\\r">>;
escape($')  -> <<"\\'">>;
escape($")  -> <<"\\\"">>;
escape($\\) -> <<"\\\\">>;
escape(C)   -> <<C>>.

result({updated, #mysql_result{affected = Affected, insert_id = InsertId}}) ->
    {ok, updated, {Affected, InsertId}};
result({data, #mysql_result{fields = Fields, rows = Rows}}) ->
    TupleFields = [{Name, Type} || #mysql_field{name = Name, type = Type} <- Fields],
    {ok, data, {TupleFields, Rows}};
result({error, #mysql_error{message = Message}}) ->
    {error, Message};
result(Result) ->
    Result.

