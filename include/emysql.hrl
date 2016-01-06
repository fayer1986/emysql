%%--------------------------------------------------------------------
%% MySQL Result
%%--------------------------------------------------------------------

-record(mysql_field, {table, name, length, type}).

-type mysql_field() :: #mysql_field{}.

-record(mysql_result, {
        fields    = [],
        rows      = [],
        affected  = 0,
        insert_id = 0,
        error}).

-type mysql_result() :: #mysql_result{}.

-record(mysql_error, {code, message}).

-type mysql_error() :: #mysql_error{}.

