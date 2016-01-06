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
%%% @doc MySQL Client.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------

-module(emysql_client).

-include("emysql.hrl").

-include("emysql_packet.hrl").

-behaviour(gen_server).

-export([connect/1, info/1, query/2, query/3, prepare/3,
         execute/3, execute/4, unprepare/2, close/1]).

%% gen_server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-type host() :: inet:ip_address() | inet:hostname() | string().

-type option() :: {host, host()}
                | {port, inet:port_number()}
                | {username, string()}
                | {password, string()}
                | {database, string()}
                | {ssl,      boolean()}
                | {ssl_opts, [ssl:ssl_option()]}
                | {encode, atom()}
                | {timeout, pos_integer()}
                | {logger,   atom() | {atom(), atom()}}.

-export_type([option/0]).

-define(TIMEOUT, 600000).

-record(state, {host      = "localhost" :: host(),
                port      = 3306        :: inet:port_number(),
                username  = <<>>        :: binary(),
                password  = <<>>        :: binary(),
                database                :: binary(),
                encoding  = utf8        :: atom(),
                version   = ?MYSQL_4_1  :: pos_integer(),
                ssl_opts  = []          :: [ssl:ssl_option()],
                transport = tcp         :: tcp | ssl,
                socket                  :: inet:socket(),
                receiver                :: pid(),
                prepared                :: dict:dict(),
                seqnum                  :: integer(),
                timeout   = ?TIMEOUT    :: pos_integer(),
                logger                  :: gen_logger:logmod()}).

-type query_result() :: {updated, mysql_result()}
                      | {data, mysql_result()}
                      | {error, any()}.

%%%=============================================================================
%%% API Functions
%%%=============================================================================

%% @doc Connect to MySQL.
-spec connect([option()]) -> {ok, pid()} | {error, any()}.
connect(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

%% @doc SQL Query
-spec query(pid(), binary()) -> query_result().
query(C, Sql) when is_binary(Sql) ->
    call(C, {query, Sql}).

-spec query(pid(), binary(), timeout()) -> query_result().
query(C, Sql, Timeout) when is_binary(Sql) ->
    call(C, {query, Sql}, Timeout).

%% @doc Prepare
-spec prepare(pid(), binary(), binary()) -> query_result().
prepare(C, Name, Stmt) when is_binary(Name), is_binary(Stmt) ->
    call(C, {prepare, Name, Stmt}).

%% @doc Execute
-spec execute(pid(), binary(), binary()) -> query_result().
execute(C, Name, Params) when is_binary(Name) ->
    call(C, {execute, Name, Params}).

-spec execute(pid(), binary(), binary(), timeout()) -> query_result().
execute(C, Name, Params, Timeout) when is_binary(Name) ->
    call(C, {execute, Name, Params}, Timeout).

%% @doc Unprepare
-spec unprepare(pid(), binary()) -> query_result().
unprepare(C, Name) when is_binary(Name) ->
    call(C, {unprepare, Name}).

%% @doc Client Info
-spec info(pid()) -> list().
info(C) -> call(C, info).

%% @doc Close the client.
-spec close(pid()) -> ok.
close(C) -> call(C, close).

call(C, Request) ->
    gen_server:call(C, Request, infinity).

call(C, Request, Timeout) ->
    gen_server:call(C, Request, Timeout).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Opts]) ->

    Logger = gen_logger:new({console, info}),

    case connect_mysql(parse_opt(Opts, #state{logger = Logger})) of
        {ok, Greeting, State} ->
            case auth_mysql(Greeting, State) of
                {ok, State1}   -> init_mysql(State1);
                {error, Error} -> {stop, Error}
            end;
        {error, Error} ->
            {stop, Error}
    end.

parse_opt([], State) ->
    State;
parse_opt([{host, Host} | Opts], State) ->
    parse_opt(Opts, State#state{host = Host});
parse_opt([{port, Port} | Opts], State) when is_integer(Port) ->
    parse_opt(Opts, State#state{port = Port});
parse_opt([{username, Username} | Opts], State) ->
    parse_opt(Opts, State#state{username = iolist_to_binary(Username)});
parse_opt([{password, Password} | Opts], State) ->
    parse_opt(Opts, State#state{password = iolist_to_binary(Password)});
parse_opt([{database, Database} | Opts], State) ->
    parse_opt(Opts, State#state{database = iolist_to_binary(Database)});
parse_opt([{encoding, Encoding} | Opts], State) when is_atom(Encoding) ->
    parse_opt(Opts, State#state{encoding = Encoding});
parse_opt([Ssl | Opts], State) when Ssl =:= ssl orelse Ssl =:= {ssl, true} ->
    ssl:start(),
    parse_opt(Opts, State#state{transport = ssl});
parse_opt([{ssl_opts, SslOpts} | Opts], State) ->
    parse_opt(Opts, State#state{ssl_opts = SslOpts});
parse_opt([{logger, Cfg} | Opts], State) ->
    parse_opt(Opts, State#state{logger = gen_logger:new(Cfg)});
parse_opt([{timeout, Timeout} | Opts], State) ->
    parse_opt(Opts, State#state{timeout = timer:seconds(Timeout)});
parse_opt([Opt | _Opts], _State) ->
    throw({badopt, Opt}).

handle_call(info, _From, State = #state{host     = Host,
                                        port     = Port,
                                        username = Username,
                                        password = Password,
                                        database = Database,
                                        encoding = Encoding,
                                        version  = Version}) ->
    Info = [{host, Host}, {port, Port},
            {username, Username},
            {password, Password},
            {database, Database},
            {encoding, Encoding},
            {verion, Version}],
    {reply, Info, State, hibernate};

handle_call(close, _From, State) ->
    {stop, normal, ok, State};

handle_call({query, Query}, _From, State) ->
    reply(do_query(Query, State), State);

handle_call({prepare, Name, Stmt}, _From, State) ->
    Prepare = <<"PREPARE ", Name/binary, " FROM ",
                (emysql_type:quote(Stmt))/binary>>,
    reply(do_query(Prepare, State), State);

handle_call({unprepare, Name}, _From, State) ->
    Unprepare = <<"DEALLOCATE PREPARE ", Name/binary>>,
    reply(do_query(Unprepare, State), State);

handle_call({execute, Name, Params}, _From, State) ->
    Stmts = make_statements(Name, Params),
    reply(do_queries(Stmts, State), State);

handle_call(Req, _From, State = #state{logger = Logger}) ->
    Logger:error("Unexepcted request: ~p", [Req]),
    reply({error, unexpected_req}, State).

handle_cast(Msg, State = #state{logger = Logger}) ->
    Logger:error("Unexepcted msg: ~p", [Msg]),
    {noreply, State}.

handle_info({mysql_recv, _Receiver, packet, Packet},
            State = #state{logger = Logger}) ->
    Logger:error("Unexpected mysql_recv: ~p", [Packet]),
    {noreply, State};

handle_info(Info, State = #state{logger = Logger}) ->
    Logger:error("Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{socket = Sock}) ->
    emysql_sock:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal Function Definitions
%%%=============================================================================

connect_mysql(State = #state{transport = Transport, host = Host, port = Port,
                             ssl_opts = SslOpts, logger = Logger}) ->
    case emysql_sock:connect(self(), Transport, Host, Port, SslOpts, Logger) of
        {ok, Sock, Receiver} ->
            NewState = State#state{socket = Sock, receiver = Receiver},
            case wait_for_greeting(NewState) of
                {ok, Greeting = #mysql_greeting{version = Version}} ->
                    {ok, Greeting, NewState#state{version = Version}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

wait_for_greeting(State) ->
    case recv(undefined, State) of
        {ok, Seq, Packet} ->
            Greeting = emysql_packet:parse_greeting(Packet),
            {ok, Greeting#mysql_greeting{seqnum = Seq}};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Authentication
%%--------------------------------------------------------------------

auth_mysql(#mysql_greeting{seqnum = Seq, salt1 = Salt1,
                           salt2 = Salt2, caps = Caps}, State) ->
    NextSeq = Seq + 1,
    auth_result(
        case emysql_auth:is_secure_passwd(Caps) of
            true  -> auth_new(NextSeq, Salt1, Salt2, State);
            false -> auth_old(NextSeq, Salt1, State)
        end, State).

auth_old(Seq, Salt1, State = #state{username = Username, password = Password}) ->
    Auth = emysql_auth:password_old(Password, Salt1),
    Packet = emysql_auth:make_auth(Username, Auth),
    command(Seq, Packet, State).

auth_new(Seq, Salt1, Salt2, State = #state{username = Username, password = Password}) ->
    Auth = emysql_auth:password_new(Password, <<Salt1/binary, Salt2/binary>>),
    Packet2 = emysql_auth:make_new_auth(Username, Auth, undefined),
    case command(Seq, Packet2, State) of
        {ok, SeqNum2, <<254:8>>} ->
            AuthOld = emysql_auth:password_old(Password, Salt1),
            command(SeqNum2 + 1, <<AuthOld/binary, 0:8>>, State);
        {ok, SeqNum2, Packet3} ->
            {ok, SeqNum2, Packet3};
        {error, Reason} ->
            {error, Reason}
    end.

auth_result({ok, SeqNum, <<0:8, _/binary>>}, State) ->
    {ok, State#state{seqnum = SeqNum}};
auth_result({ok, _, <<255:8, _Code:16/little, Message/binary>>}, _) ->
    {error, {auth_failed, Message}};
auth_result({ok, _, Packet}, _State) ->
    {error, {auth_failed, Packet}};
auth_result({error, Reason}, _State) ->
    {error, Reason}.

init_mysql(State = #state{database = Database, encoding = Encoding}) ->
    EncBin = list_to_binary(atom_to_list(Encoding)),
    do_queries([<<"use ", Database/binary>>,
                <<"set names ", (emysql_type:quote(EncBin))/binary>>], State),
    {ok, State}.

do_queries(Queries, State) ->
    catch lists:foldl(
        fun(Query, _LastResp) ->
            case do_query(Query, State) of
                {error, _} = Err -> throw(Err);
                Res -> Res
            end
        end, ok, Queries).

do_query(Query, State = #state{logger = Logger}) ->
    Logger:info("[emysql~p] Query: ~s", [self(), Query]),
    case send(0, <<?MYSQL_QUERY_OP, Query/binary>>, State) of
        ok    -> recv_response(State);
        Error -> Error
    end.

recv_response(State) ->
    with_recv(fun handle_response/3, State).

handle_response(_Seq, <<0, Bin/binary>>, _State) ->
    {updated, emysql_packet:parse_update_result(Bin)};
handle_response(_Seq, <<255, Rest/binary>>, _State) ->
    {error, emysql_packet:parse_error(Rest)};
handle_response(_Seq, _Packet, State) ->
    %% Tabular data received
    case recv_fields([], State) of
        {ok, Fields} ->
            case recv_rows(Fields, [], State) of
                {ok, Rows} ->
                    {data, #mysql_result{fields = Fields, rows = Rows}};
                {error, Reason} ->
                    {error, Reason}
                end;
        {error, Reason} ->
            {error, Reason}
    end.

recv_fields(Fields, State) ->
    with_recv(fun handle_fields/4, Fields, State).

handle_fields(_Seq, <<254>>, Fields, _State) ->
    {ok, lists:reverse(Fields)};
handle_fields(_Seq, <<254, Rest/binary>>, Fields, _State) when size(Rest) < 8 ->
    {ok, lists:reverse(Fields)};
handle_fields(_Seq, Packet, Fields, State = #state{version = Version}) ->
    Field = emysql_packet:parse_field(Version, Packet),
    recv_fields([Field | Fields], State).

recv_rows(Fields, Rows, State) ->
    with_recv(fun handle_rows/4, {Fields, Rows}, State).

handle_rows(_Seq, <<254:8, Rest/binary>>, {_, Rows}, _State) when size(Rest) < 8 ->
    {ok, lists:reverse(Rows)};
handle_rows(_Seq, Packet, {Fields, Rows}, State) ->
    recv_rows(Fields, [emysql_packet:parse_row(Packet, Fields) | Rows], State).

with_recv(Fun, State) ->
    case recv(undefined, State) of
        {ok, Seq, Packet} -> Fun(Seq, Packet, State);
        {error, Reason}   -> {error, Reason}
    end.

with_recv(Fun, Acc, State) ->
    case recv(undefined, State) of
        {ok, Seq, Packet} -> Fun(Seq, Packet, Acc, State);
        {error, Reason}   -> {error, Reason}
    end.

make_statements(Name, []) ->
    [<<"EXECUTE ", Name/binary>>];

make_statements(Name, Params) ->
    ParamNums = lists:seq(1, length(Params)),
    ParamNames = [[$@ | integer_to_list(I)] || I <- ParamNums],
    ParamsBin = list_to_binary(string:join(ParamNames, ",")),
    ExecStmt = <<"EXECUTE ", Name/binary, " USING ", ParamsBin/binary>>,

    SetFun = fun({Num, Val}) -> <<"SET @", (emysql_type:encode(Num))/binary, "=", (emysql_type:encode(Val))/binary>> end,
                
    ParamVals = [SetFun({Num, Val})  || {Num, Val} <- lists:zip(ParamNums, Params)],

    ParamVals ++ [ExecStmt].

%% command(Packet, State) -> command(undefined, Packet, State).

command(Seq, Packet, State) ->
    case send(Seq, Packet, State) of
        ok    -> recv(Seq, State);
        Error -> Error
    end.

send(Seq, Packet, #state{socket = Sock, logger = Logger}) ->
    Data = emysql_packet:serialize({Seq, Packet}),
    Logger:debug("[emysql~p] SEND: ~p", [self(), Data]),
    emysql_sock:send(Sock, Data).

recv(undefined, #state{receiver = Receiver, timeout = Timeout}) ->
    receive
    {mysql_recv, Receiver, packet, {Seq, Packet}} ->
        {ok, Seq, Packet}
    after Timeout ->
        {error, mysql_timeout}
    end;

recv(Seq, #state{receiver = Receiver, timeout = Timeout}) when is_integer(Seq) ->
    RecvSeq = Seq + 1,
    receive
    {mysql_recv, Receiver, packet, {RecvSeq , Packet}} ->
        {ok, RecvSeq, Packet}
    after Timeout ->
        {error, mysql_timeout}
    end.

reply({error, mysql_timeout} = Error, State) ->
    {stop, mysql_timeout, Error, State};

reply(Result, State) ->
    {reply, Result, State, hibernate}.

