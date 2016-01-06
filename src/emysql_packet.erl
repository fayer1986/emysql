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
%%% @author Feng Lee <feng@emqtt.io>
%%%
%%% @doc MySQL Packet Parser and Serializer:
%%%  
%%% Type        |  Name           | Description
%%% ----------------------------------------------------------------------------
%%% int<3>      | payload_length  | Length of the payload. 
%%% int<1>      | sequence_id     | Sequence ID
%%% string<var> | payload         | [len=payload_length] payload of the packet
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emysql_packet).

-include("emysql.hrl").

-include("emysql_packet.hrl").

-export([parser/0, parse_greeting/1, parse_update_result/1, parse_string/1,
         parse_error/1, parse_field/2, parse_row/2, parse_length/1, serialize/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(HEADER_LEN, 4).

-type packet() :: {byte(), binary()}.

-type parse_fun() :: fun((binary()) -> {ok, packet()} | {more, parse_fun()}).

%% @doc Initialize a parser
-spec parser() -> parse_fun().
parser() ->
    fun(Bin) -> parse(Bin, none) end.

%% @doc Parse Packet
parse(<<>>, none) ->
    {more, fun(Bin) -> parse(Bin, none) end};

parse(Bin, none) when size(Bin) < ?HEADER_LEN ->
    {more, fun(More) -> parse(<<Bin/binary, More/binary>>, none) end};

parse(<<Len:24/little, Seq:8, Bin/binary>>, none) ->
    parse_body(Bin, Seq, Len).

parse_body(Bin, Seq, Len) when size(Bin) < Len ->
    {more, fun(More) -> parse_body(<<Bin/binary, More/binary>>, Seq, Len) end};

parse_body(Bin, Seq, Len) ->
    <<Payload:Len/binary, Rest/binary>> = Bin,
    {ok, {Seq, Payload}, Rest}.

parse_greeting(Bin) ->
    <<Protocol:8, Rest/binary>> = Bin,
    {Version, Rest2} = asciz(Rest),
    <<ThreadID:32/little, Rest3/binary>> = Rest2,
    {Salt, Rest4} = asciz(Rest3),
    <<Caps:16/little, Rest5/binary>> = Rest4,
    <<ServerChar:16/binary-unit:8, Rest6/binary>> = Rest5,
    {Salt2, _Rest7} = asciz(Rest6),
    #mysql_greeting{protocol = Protocol,
                    version = parse_version(Version),
                    thread = ThreadID, salt1 = Salt, caps = Caps,
                    server = ServerChar, salt2 = Salt2}.

parse_version(<<"4.0", _/binary>>) ->
    ?MYSQL_4_0;
parse_version(<<"4.1", _/binary>>) ->
    ?MYSQL_4_1;
parse_version(<<"5", _/binary>>) ->
    %% MySQL version 5.x protocol is compliant with MySQL 4.1.x:
    ?MYSQL_4_1; 
parse_version(<<"6", _/binary>>) ->
    %% MySQL version 6.x protocol is compliant with MySQL 4.1.x:
    ?MYSQL_4_1; 
parse_version(_Other) ->
    ?MYSQL_4_0.

asciz(Bin) -> asciz(Bin, <<>>).

%% @doc Find the first zero-byte in Data and add everything before it to Acc.
asciz(<<>>, Acc) ->
    {Acc, <<>>};
asciz(<<0:8, Rest/binary>>, Acc) ->
    {Acc, Rest};
asciz(<<C:8, Rest/binary>>, Acc) ->
    asciz(Rest, <<Acc/binary, C:8>>).

parse_field(?MYSQL_4_0, Bin) ->
    {Table, Rest} = parse_string(Bin),
    {Field, Rest2} = parse_string(Rest),
    {LengthB, Rest3} = parse_string(Rest2),
    LengthL = size(LengthB) * 8,
    <<Length:LengthL/little>> = LengthB,
    {Type, Rest4} = parse_string(Rest3),
    {_Flags, _Rest5} = parse_string(Rest4),
    #mysql_field{table = Table, name = Field, length = Length,
                 type = emysql_type:name(Type)};

parse_field(?MYSQL_4_1, Bin) ->
    {_Catalog, Rest} = parse_string(Bin),
    {_Database, Rest2} = parse_string(Rest),
    {Table, Rest3} = parse_string(Rest2),
    %% OrgTable is the real table name if Table is an alias
    {_OrgTable, Rest4} = parse_string(Rest3),
    {Field, Rest5} = parse_string(Rest4),
    %% OrgField is the real field name if Field is an alias
    {_OrgField, Rest6} = parse_string(Rest5),
    <<_Metadata:8/little, _Charset:16/little,
     Length:32/little, Type:8/little,
     _Flags:16/little, _Decimals:8/little,
     _Rest7/binary>> = Rest6,
    #mysql_field{table = Table, name = Field, length = Length,
                  type = emysql_type:name(Type)}.

parse_update_result(Bin) ->
    %% No Tabular data
    {AffectedRows, Rest} = parse_length(Bin),
    {InsertId, _} = parse_length(Rest),
    #mysql_result{affected = AffectedRows, insert_id = InsertId}.

parse_row(Bin, Fields) ->
    parse_row(Bin, Fields, []).

parse_row(_Bin, [], Cells) ->
    lists:reverse(Cells);

parse_row(Bin, [Field | Fields], Cells) ->
    {Col, Rest} = parse_string(Bin),
    Cell = parse_cell(Field, Col),
    parse_row(Rest, Fields, [Cell | Cells]).

parse_cell(_Field, null) ->
    null;
parse_cell(#mysql_field{type = Type}, Val) when ?IS_INTEGER(Type) ->
    list_to_integer(binary_to_list(Val));
parse_cell(#mysql_field{type = Type}, Val) when ?IS_DATETIME(Type) ->
    {ok, [Y, M, D, H, MM, S], _} = io_lib:fread("~d-~d-~d ~d:~d:~d", binary_to_list(Val)),
    {datetime, {{Y, M, D}, {H, MM, S}}};
parse_cell(#mysql_field{type = 'TIME'}, Val) ->
    {ok, [H, M, S], _} =
    io_lib:fread("~d:~d:~d", binary_to_list(Val)),
    {time, {H, M, S}};
parse_cell(#mysql_field{type = 'DATE'}, Val) ->
    {ok, [Y, M, D], _} = io_lib:fread("~d-~d-~d", binary_to_list(Val)),
    {date, {Y, M, D}};
parse_cell(#mysql_field{type = T}, Val) when ?IS_FLOAT(T) ->
    {ok, [Num], _} =
    case io_lib:fread("~f", binary_to_list(Val)) of
        {error, _} ->
            io_lib:fread("~d", binary_to_list(Val));
        Res -> Res
    end, Num;
parse_cell(_Field, Val) ->
    Val.

parse_error(<<Code:16/little, Message/binary>>) ->
    #mysql_error{code = Code, message = Message}.

parse_string(<<Len, Str:Len/binary, Rest/binary>>) when Len < 251 ->
    {Str, Rest};
parse_string(<<251, Bin/binary>>) ->
    {null, Bin};
parse_string(<<252, Len:16/little, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest};
parse_string(<<253, Len:24/little, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest};
parse_string(<<254, Len:64/little, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

parse_length(<<Len, Rest/binary>>) when Len =< 251 ->
    {Len, Rest};
%% two bytes
parse_length(<<252, Val:16/little, Rest/binary>>) ->
    {Val, Rest};
%% three bytes
parse_length(<<253, Val:24/little, Rest/binary>>) ->
    {Val, Rest};
parse_length(<<254, Val:64/little, Rest/binary>>) ->
    {Val, Rest};
parse_length(Bin) ->
    {0, Bin}.

serialize({SeqNum, Payload}) ->
    <<(size(Payload)):24/little, SeqNum:8, Payload/binary>>.

