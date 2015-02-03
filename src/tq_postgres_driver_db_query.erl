%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Feb 2015 9:31 AM
%%%-------------------------------------------------------------------
-module(tq_postgres_driver_db_query).
-author("isergey").

%% API
-export([query/4, squery/3, transaction/2]).
-include_lib("../../epgsql/include/pgsql.hrl").

query(Conn, Sql, EscapedArgs, Constructor) ->
    case pgsql:equery(Conn, Sql, EscapedArgs) of
        {ok, Count} when is_integer(Count) ->
            {ok, Count};
        {ok, _Columns, Rows} ->
            {ok, [Constructor(tuple_to_list(R)) || R <- Rows]};
        {ok, Count, _Columns, Rows} ->
            {ok, Count, [Constructor(tuple_to_list(R)) || R <- Rows]};
        {error, Reason} ->
            Reason2 = transform_error(Sql, EscapedArgs, Reason),
            {error, Reason2}
    end.

squery(Conn, Sql, Constructor)->
    case pgsql:squery(Conn, Sql) of
        {ok, Count} when is_integer(Count) ->
            {ok, Count};
        {ok, _Columns, Rows} ->
            {ok, [Constructor(tuple_to_list(R)) || R <- Rows]};
        {ok, Count, _Columns, Rows} ->
            {ok, Count, [Constructor(tuple_to_list(R)) || R <- Rows]};
        {error, Reason} ->
            Reason2 = transform_error(Sql, Reason),
            {error, Reason2};
        List when is_list(List) ->
            lists:foldl(fun transform_answer/2, ok, List)
    end.

transaction(Conn, Fun) ->
    pgsql:with_transaction(Conn, Fun).

%%%===================================================================
%%% Internal functions
%%%===================================================================
transform_answer(X, Answer) ->
    case X of
        {ok, _} -> Answer;
        {error, Reason} ->
            Reason2 = transform_error(Reason),
            [{error, Reason2}|Answer]
    end.

transform_error(_Sql, _Args, #error{code = <<"23505">>}) ->
    not_unique;
transform_error(Sql, Args, Error) ->
    {db_error,
        [
            {code, Error#error.code},
            {message, Error#error.message},
            {extra, Error#error.extra},
            {sql, Sql},
            {args, Args}
        ]}.

transform_error(Error) ->
    {db_error,
        [
            {code, Error#error.code},
            {message, Error#error.message},
            {extra, Error#error.extra}
        ]}.

transform_error(Sql, Error) ->
    {db_error,
        [
            {code, Error#error.code},
            {message, Error#error.message},
            {extra, Error#error.extra},
            {sql, Sql}
        ]}.
