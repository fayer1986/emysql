%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2016 eMQTT.IO, All Rights Reserved.
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
%%% @doc MySQL Types
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------

-module(emysql_type).

-include("emysql_packet.hrl").

-export([name/1, value/1, encode/1, quote/1]).

name(?DECIMAL)     -> 'DECIMAL';
name(?TINY)        -> 'TINY';
name(?SHORT)       -> 'SHORT';
name(?LONG)        -> 'LONG';
name(?FLOAT)       -> 'FLOAT';
name(?DOUBLE)      -> 'DOUBLE';
name(?NULL)        -> 'NULL';
name(?TIMESTAMP)   -> 'TIMESTAMP';
name(?LONGLONG)    -> 'LONGLONG';
name(?INT24)       -> 'INT24';
name(?DATE)        -> 'DATE';
name(?TIME)        -> 'TIME';
name(?DATETIME)    -> 'DATETIME';
name(?YEAR)        -> 'YEAR';
name(?NEWDATE)     -> 'NEWDATE';
name(?NEWDECIMAL)  -> 'NEWDECIMAL';
name(?ENUM)        -> 'ENUM';
name(?SET)         -> 'SET';
name(?TINYBLOB)    -> 'TINYBLOB';
name(?MEDIUM_BLOG) -> 'MEDIUM_BLOG';
name(?LONG_BLOG)   -> 'LONG_BLOG';
name(?BLOB)        -> 'BLOB';
name(?VAR_STRING)  -> 'VAR_STRING';
name(?STRING)      -> 'STRING';
name(?GEOMETRY)    -> 'GEOMETRY'.

value('DECIMAL')     -> ?DECIMAL;
value('TINY')        -> ?TINY;
value('SHORT')       -> ?SHORT;
value('LONG')        -> ?LONG;
value('FLOAT')       -> ?FLOAT;
value('DOUBLE')      -> ?DOUBLE;
value('NULL')        -> ?NULL;
value('TIMESTAMP')   -> ?TIMESTAMP;
value('LONGLONG')    -> ?LONGLONG;
value('INT24')       -> ?INT24;
value('DATE')        -> ?DATE;
value('TIME')        -> ?TIME;
value('DATETIME')    -> ?DATETIME;
value('YEAR')        -> ?YEAR;
value('NEWDATE')     -> ?NEWDATE;
value('NEWDECIMAL')  -> ?NEWDECIMAL;
value('ENUM')        -> ?ENUM;
value('SET')         -> ?SET;
value('TINYBLOB')    -> ?TINYBLOB;
value('MEDIUM_BLOG') -> ?MEDIUM_BLOG;
value('LONG_BLOG')   -> ?LONG_BLOG;
value('BLOB')        -> ?BLOB;
value('VAR_STRING')  -> ?VAR_STRING;
value('STRING')      -> ?STRING;
value('GEOMETRY')    -> ?GEOMETRY.

encode(Val) when Val == undefined; Val == null ->
    <<"NULL">>;
encode(Val) when is_atom(Val) ->
    encode(atom_to_list(Val));
encode(Val) when is_integer(Val) ->
    list_to_binary(integer_to_list(Val));
encode(Val) when is_float(Val) ->
    iolist_to_binary(io_lib:format("~w", [Val]));
encode(Val) when is_list(Val) ->
    quote(list_to_binary(Val));
encode(Val) when is_binary(Val) ->
    quote(Val);
encode({datetime, {{Y, M, D}, {H, MM, S}}}) ->
    iolist_to_binary(io_lib:format("~4..0w~2..0w~2..0w~2..0w~2..0w~2..0w",
                                   [Y, M, D, H, MM, S]));
encode({date, {Y, M, D}}) ->
    iolist_to_binary(io_lib:format("~2..0w~2..0w~2..0w", [Y, M, D]));
encode({time, {H, M, S}}) -> 
    iolist_to_binary(io_lib:format("~2..0w~2..0w~2..0w", [H, M, S]));
encode(Val) ->
    throw({unrecognized_value, Val}).

%%  Quote a string or binary value so that it can be included safely in a
%%  MySQL query.
quote(Bin) when is_binary(Bin) ->
    <<39, (quote(Bin, <<>>))/binary, 39>>.
quote(<<>>, Acc) ->
    Acc;
quote(<<0, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\, $0>>);
quote(<<10, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\, $n>>);
quote(<<13, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\, $r>>);
quote(<<$\\, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\ , $\\>>);
%% 39 is $'
quote(<<39, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\, 39>>);
%% 34 is $"
quote(<<34, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\, 34>>);
quote(<<26, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, $\\, $Z>>);
quote(<<C, Rest/binary>>, Acc) ->
    quote(Rest, <<Acc/binary, C>>).

