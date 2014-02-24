-module(tq_postgres_driver).

-include_lib("epgsql/include/pgsql.hrl").

-export([
         start_pool/2,

         'query'/4,
         'parse'/2,

         find/4,
         get/3,
         insert/3,
         update/4,
         delete/3
        ]).

%% =============================================================================
%% API functions
%% =============================================================================

start_pool(PoolName, Opts) ->
    PoolArgs =
        [
         {name, {local, PoolName}},
         {worker_module, tq_postgres_driver_worker}
         | Opts
        ],
    Spec = poolboy:child_spec(PoolName, PoolArgs, Opts),
    supervisor:start_child(tq_postgres_driver_sup, Spec).

'query'(PoolName, Sql, Args, Constructor) ->
    case escape_args(Args) of
        {ok, EscapedArgs} ->
            poolboy:transaction(
              PoolName,
              fun(Worker) ->
                      tq_postgres_driver_worker:'query'(
                        Worker, Sql, EscapedArgs, Constructor)
              end);
        {error, _Reason} = Err ->
            Err
    end.

parse(Query, QueryArgs) ->
    tq_postgres_driver_dsl:parse(Query, QueryArgs).

find(PoolName, Module, Query, QueryArgs) ->
    tq_postgres_driver_runtime:find(PoolName, Module, Query, QueryArgs).

get(PoolName, Module, IndexFV) ->
    tq_postgres_driver_runtime:get(PoolName, Module, IndexFV).

insert(PoolName, Module, ChangedFV) ->
    tq_postgres_driver_runtime:insert(PoolName, Module, ChangedFV).

update(PoolName, Module, ChangedFV, IndexFV) ->
    tq_postgres_driver_runtime:update(PoolName, Module, ChangedFV, IndexFV).

delete(PoolName, Module, IndexFV) ->
    tq_postgres_driver_runtime:delete(PoolName, Module, IndexFV).

%% =============================================================================
%% Internal functions
%% =============================================================================

escape_args(Args) ->
    error_writer_map(fun escape_arg/1, Args).

escape_arg({_Name, null}) ->
    {ok, null};
escape_arg({Name, Arg}) when
      Name =:= smallint;
      Name =:= int2;
      Name =:= integer;
      Name =:= int4;
      Name =:= bigint;
      Name =:= int8
      ->
    case Arg of
        Bin when is_binary(Bin) ->
            try
                {ok, binary_to_integer(Bin)}
            catch _ ->
                    {error, bad_arg}
            end;
        Int when is_integer(Int) ->
            {ok, Int};
        _ ->
            {error, bad_arg}
    end;
escape_arg({Name, Arg}) when
      Name =:= real;
      Name =:= float4;
      Name =:= float8 ->
    case Arg of
        Bin when is_binary(Bin) ->
            try
                {ok, binary_to_integer(Bin)}
            catch _ ->
                    try
                        {ok, binary_to_float(Bin)}
                    catch _ ->
                            {error, bad_arg}
                    end
            end;
        Int when is_integer(Int) ->
            {ok, Int};
        Float when is_float(Float) ->
            {ok, Float};
        _ ->
            {error, bad_arg}
    end;
escape_arg({Name, Arg}) when
      Name =:= string;
      Name =:= text;
      Name =:= varchar ->
    case Arg of
        Bin when is_binary(Bin) ->
            {ok, Bin};
        _ ->
            {error, bad_arg}
    end;
escape_arg({datetime, Arg}) ->
    case Arg of
        {{_Y, _M, _D}, {_Hh, _Mm, _Ss}} ->
            {ok, Arg};
        _ ->
            {error, bad_arg}
    end;
escape_arg({Name, Arg}) when
      Name =:= boolean;
      Name =:= bool ->
    case Arg of
        true ->
            {ok, true};
        false ->
            {ok, false};
        _ ->
            {error, bad_arg}
    end.

error_writer_map(Fun, List) ->
    error_writer_map(Fun, List, [], []).

error_writer_map(_Fun, [], Acc, []) ->
    {ok, lists:reverse(Acc)};
error_writer_map(_Fun, [], _Acc, Errors) ->
    {error, Errors};
error_writer_map(Fun, [H|T], Acc, Errors) ->
    case Fun(H) of
        {ok, R} ->
            error_writer_map(Fun, T, [R|Acc], Errors);
        {error, R} ->
            error_writer_map(Fun, T, Acc, [Errors|R])
    end.
