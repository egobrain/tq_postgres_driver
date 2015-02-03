-module(tq_postgres_driver).

-include_lib("epgsql/include/pgsql.hrl").

-export([
         start_pool/3,

         'squery'/2,
         'squery'/3,
         'query'/4,
         'transaction'/2,
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

start_pool(PoolName, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [
         {name, {local, PoolName}},
         {worker_module, tq_postgres_driver_worker}
         | SizeArgs
        ],
    Spec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),
    supervisor:start_child(tq_postgres_driver_sup, Spec).

'squery'(PoolName, Sql) ->
    Constructor = fun(A) -> A end,
    'squery'(PoolName, Sql, Constructor).

'squery'(PoolName, Sql, Constructor) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            tq_postgres_driver_worker:'squery'(
                Worker, Sql, Constructor)
        end).

'query'(PoolName, Sql, Args, Constructor) ->
    case tq_postgres_driver_utils:escape_args(Args) of
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

'transaction'(PoolName, Fun) ->
    poolboy:transaction(
        PoolName,
        fun(Worker) ->
            tq_postgres_driver_worker:transaction(
                Worker, Fun)
        end).

parse(Query, QueryArgs) ->
    tq_postgres_driver_dsl:parse(Query, QueryArgs).

find(Interface, Module, Query, QueryArgs) ->
    tq_postgres_driver_runtime:find(Interface, Module, Query, QueryArgs).

get(Interface, Module, IndexFV) ->
    tq_postgres_driver_runtime:get(Interface, Module, IndexFV).

insert(Interface, Module, ChangedFV) ->
    tq_postgres_driver_runtime:insert(Interface, Module, ChangedFV).

update(Interface, Module, ChangedFV, IndexFV) ->
    tq_postgres_driver_runtime:update(Interface, Module, ChangedFV, IndexFV).

delete(Interface, Module, IndexFV) ->
    tq_postgres_driver_runtime:delete(Interface, Module, IndexFV).
