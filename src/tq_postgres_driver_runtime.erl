-module(tq_postgres_driver_runtime).

-export([
         find/4,
         get/3,
         insert/3,
         update/4,
         delete/3
        ]).

%% =============================================================================
%% Api
%% =============================================================================

get(PoolName, Module, IndexFV) when is_atom(PoolName) or is_binary(PoolName) ->
    {Sql, Args, Constructor} = get_params(Module, IndexFV),
    Result = tq_postgres_driver:'query'(PoolName, Sql, Args, Constructor),
    get_result(Result);

get(Connection, Module, IndexFV) when is_pid(Connection) ->
    {Sql, Args, Constructor} = get_params(Module, IndexFV),
    Result = query_with_escape(Connection, Sql, Args, Constructor),
    get_result(Result).

get_result(Result) ->
    case Result of
        {ok, [R]} ->
            {ok, R};
        {ok, []} ->
            {error, undefined};
        {error, _Reason} = Err ->
            Err
    end.

get_params(Module, IndexFV) ->
    Table = Module:'$meta'(table),

    RFields = Module:'$meta'({db_fields, r}),
    <<$,, Fields/binary>> =
        << <<$,, (Module:'$meta'({db_alias, F}))/binary>> || F <- RFields>>,
    Constructor = Module:constructor(RFields),

    {<<" AND ", Where/binary>>, _} =
        each_with(
          fun({F, _V}, I, Acc) ->
                  Alias = Module:'$meta'({db_alias, F}),
                  BinIndex = integer_to_binary(I),
                  <<Acc/binary, " AND ", Alias/binary, " = $", BinIndex/binary>>
          end, <<>>, IndexFV),

    Sql =
        <<"SELECT ", Fields/binary,
          " FROM ", Table/binary,
          " WHERE ", Where/binary, ";">>,
    Args = [{Module:'$meta'({db_type, F}), V} || {F, V} <- IndexFV],
    {Sql, Args, Constructor}.

find(PoolName, Module, Query, QueryArgs) when is_atom(PoolName) or is_binary(PoolName) ->
    case find_params(Module, Query, QueryArgs) of
        {Sql, Args, Constructor} ->
            tq_postgres_driver:'query'(PoolName, Sql, Args, Constructor);
        {error, _Reason} = Err ->
            Err
    end;

find(Connection, Module, Query, QueryArgs) when is_pid(Connection) ->
    case find_params(Module, Query, QueryArgs) of
        {Sql, Args, Constructor} ->
            query_with_escape(Connection, Sql, Args, Constructor);
        {error, _Reason} = Err ->
            Err
    end.

find_params(Module, Query, QueryArgs) ->
    case tq_postgres_driver_dsl:parse(Module, Query) of
        {ok, {Where, _Fields, ArgFuns}} ->
            Table = Module:'$meta'(table),
            RFields = Module:'$meta'({db_fields, r}),
            <<$,, Fields/binary>> =
                << <<$,, Table/binary, $., (Module:'$meta'({db_alias, F}))/binary>>
                    || F <- RFields>>,
            Sql =
                <<"SELECT ", Fields/binary,
                  " FROM ", Table/binary,
                  " ", Where/binary, ";">>,
            Args =
                lists:zipwith(
                  fun(F, A) -> F(A) end,
                  ArgFuns,
                  QueryArgs),
            Constructor = Module:constructor(RFields),
            {Sql, Args, Constructor};
        {error, _Reason} = Err ->
            Err
    end.

delete(PoolName, Module, IndexFV)  when is_atom(PoolName) or is_binary(PoolName) ->
    {Sql, Args, Constructor} = delete_params(Module, IndexFV),
    Result = tq_postgres_driver:'query'(PoolName, Sql, Args, Constructor),
    delete_result(Result);

delete(Connection, Module, IndexFV) when is_pid(Connection) ->
    {Sql, Args, Constructor} = delete_params(Module, IndexFV),
    Result = query_with_escape(Connection, Sql, Args, Constructor),
    delete_result(Result).

delete_result(Result) ->
    case Result of
        {ok, _} ->
            ok;
        {error, _Reason} = Err ->
            Err
    end.

delete_params(Module, IndexFV) ->
    Table = Module:'$meta'(table),
    {<<" AND ", Where/binary>>, _} =
        each_with(
          fun({F, _V}, I, Acc) ->
                  Alias = Module:'$meta'({db_alias, F}),
                  BinIndex = integer_to_binary(I),
                  <<Acc/binary, " AND ", Alias/binary, " = $", BinIndex/binary>>
          end, <<>>, IndexFV),
    Sql =
        <<"DELETE FROM ", Table/binary,
          " WHERE ", Where/binary, ";">>,
    Args = [{Module:'$meta'({db_type, F}), V} || {F, V} <- IndexFV],
    Constructor = fun(A) -> A end,
    {Sql, Args, Constructor}.

insert(PoolName, Module, ChangedFV) when is_atom(PoolName) or is_binary(PoolName) ->
    {Sql, Args, Constructor} = insert_params(Module, ChangedFV),
    Result = tq_postgres_driver:'query'(PoolName, Sql, Args, Constructor),
    insert_result(Result);

insert(Connection, Module, ChangedFV) when is_pid(Connection) ->
    {Sql, Args, Constructor} = insert_params(Module, ChangedFV),
    Result = query_with_escape(Connection, Sql, Args, Constructor),
    insert_result(Result).

insert_result(Result) ->
    case Result of
        {ok, 1, [Model]} ->
            {ok, Model};
        {error, _Reason} = Err ->
            Err
    end.

insert_params(Module, ChangedFV) ->
    Table = Module:'$meta'(table),
    RFields = Module:'$meta'({db_fields, r}),
    <<$,, Returning/binary>> =
        << <<$,, (Module:'$meta'({db_alias, F}
                                ))/binary>> || F <- RFields>>,
    Constructor = Module:constructor(RFields),
    Args = [{Module:'$meta'({db_type,F}),V} || {F,V} <- ChangedFV],
    <<$,, Fields/binary>> =
        << <<$,, (Module:'$meta'({db_alias,F}))/binary>>
           || {F,_} <- ChangedFV >>,
    {<<$,, Values/binary>>, _} =
        each_with(
          fun(_, I, Acc) ->
                  BinIndex = integer_to_binary(I),
                  <<Acc/binary, ", $", BinIndex/binary>>
                      end, <<>>, ChangedFV),
    Sql = <<"INSERT INTO ", Table/binary,
            "(", Fields/binary, ")",
            " VALUES (", Values/binary, ")",
            " RETURNING ", Returning/binary, ";">>,
    {Sql, Args, Constructor}.

update(PoolName, Module, ChangedFV, IndexFV)  when is_atom(PoolName) or is_binary(PoolName) ->
    {Sql, Args, Constructor} = update_params(Module, ChangedFV, IndexFV),
    Result = tq_postgres_driver:'query'(PoolName, Sql, Args, Constructor),
    update_result(Result);

update(Connection, Module, ChangedFV, IndexFV) when is_pid(Connection) ->
    {Sql, Args, Constructor} = update_params(Module, ChangedFV, IndexFV),
    Result =  query_with_escape(Connection, Sql, Args, Constructor),
    update_result(Result).

update_result(Result) ->
    case Result of
        {ok, 1, [Model]} ->
            {ok, Model};
        {ok, 0, []} ->
            {error, undefined};
        {error, _Reason} = Err ->
            Err
    end.

update_params(Module, ChangedFV, IndexesFV) ->
    Table = Module:'$meta'(table),
    RFields = Module:'$meta'({db_fields, r}),
    <<$,, Returning/binary>> =
        << <<$,, (Module:'$meta'({db_alias, F}))/binary>> || F <- RFields>>,
    Constructor = Module:constructor(RFields),
    DataArgs = [{Module:'$meta'({db_type,F}),V} || {F,V} <- ChangedFV],
    {<<$,, Values/binary>>, Cnt} =
        each_with(
          fun({F, _V}, I, Acc) ->
                  Alias = Module:'$meta'({db_alias, F}),
                  BinIndex = integer_to_binary(I),
                  <<Acc/binary, ", ", Alias/binary, " = $", BinIndex/binary>>
          end, <<>>, ChangedFV),
    {<<" AND ", Where/binary>>, _} =
        each_with(
          fun({F, _V}, I, Acc) ->
                  Alias = Module:'$meta'({db_alias, F}),
                  BinIndex = integer_to_binary(I),
                  <<Acc/binary, " AND ", Alias/binary, " = $", BinIndex/binary>>
          end, Cnt, <<>>, IndexesFV),
    WhereArgs = [{Module:'$meta'({db_type, F}), V} || {F, V} <- IndexesFV],
    Sql =
        <<"UPDATE ", Table/binary,
          " SET ", Values/binary,
          " WHERE ", Where/binary,
          " RETURNING ", Returning/binary, ";">>,
    Args = DataArgs ++ WhereArgs,
    {Sql, Args, Constructor}.

%% =============================================================================
%%% Internal functions
%% =============================================================================

each_with(Fun, Acc, List) ->
    each_with(Fun, 1, Acc, List).
each_with(_Fun, Index, Acc, []) ->
    {Acc, Index};
each_with(Fun, Index, Acc, [H|T]) ->
    Acc2 = Fun(H, Index, Acc),
    each_with(Fun, Index+1, Acc2, T).

query_with_escape(Connection, Sql, Args, Constructor) ->
    case tq_postgres_driver_utils:escape_args(Args) of
        {ok, EscapedArgs} ->
            tq_postgres_driver_db_query:query(Connection, Sql, EscapedArgs, Constructor);
        {error, _Reason} = Err ->
            Err
    end.