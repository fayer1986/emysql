%%--------------------------------------------------------------------
%% MySQL Result
%%--------------------------------------------------------------------

-record(mysql_field, {table, name, length, type}).

-record(mysql_result, {
        fields    = [],
        rows      = [],
        affected  = 0,
        insert_id = 0,
        error}).

-record(mysql_error, {code, message}).

