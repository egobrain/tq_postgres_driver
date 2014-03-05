-module(tq_postgres_driver_dsl).

-export([
         parse/2
        ]).

-record(state, {
          acc = <<>>,
          args = [],
          fields = [],
          model,
          arg_index = 0
         }).

%% =============================================================================
%%% API functions
%% =============================================================================

parse(Model, Bin) ->
    Res = tq_dsl_parser:parse(Bin, fun sql_joiner/3, #state{model=Model}),
    case Res of
        {error, {wrong_format, {Pos, _Reasin}}} ->
            case Bin of
                <<From:Pos/binary, To/binary>> ->
                    Err = <<From/binary, "\e[31m", To/binary, "\e[0m">>,
                    io:format("~s~n", [Err]);
                _ -> ok
            end;
        _ ->
            ok
    end,
    Res.

%% =============================================================================
%%% Internal functions
%% =============================================================================

sql_joiner({string, _Pos, Str}, #state{acc=Acc}=State, Next) ->
    Acc2 = <<Acc/binary, Str/binary>>,
    Next(State#state{acc=Acc2});
sql_joiner({field_query, Pos, FieldQuery}, State, Next) ->
    field_query(none, FieldQuery, false, Pos, State, Next);
sql_joiner({field_query_alias, Pos, Link, FieldQuery}, State, Next) ->
    field_query(Link, FieldQuery, true, Pos, State, Next);
sql_joiner({field_alias, _Pos, Link, MF}, State, Next) ->
    field_alias(Link, MF, _Pos, State, Next);
sql_joiner({field_type, _Pos, MF}, #state{model=DefaultModel}=State, Next) ->
    {Model, Field} = get_model_and_field(MF, DefaultModel),
    Type = Model:'$meta'({db_type, Field}),
    TypeWrapper = fun(Val) -> {Type, Model:field_to_db(Field, Val)} end,
    arg_type(TypeWrapper, State, Next);
sql_joiner({table, _Pos, BinModel}, #state{acc=Acc}=State, Next) ->
    Model = binary_to_atom(BinModel),
    Table = Model:'$meta'(table),
    Acc2 = <<Acc/binary, Table/binary>>,
    Next(State#state{acc=Acc2});
sql_joiner({type, _Pos, BinType}, State, Next) ->
    Type = binary_to_atom(BinType),
    TypeWrapper = fun(Val) -> {Type, Val} end,
    arg_type(TypeWrapper, State, Next);
sql_joiner(finish, #state{acc=Acc, fields=Fields, args=Args}, Next) ->
    Next({Acc, lists:flatten(Fields), lists:reverse(Args)}).

get_model_and_field({BinModel, BinField}, _DefaultModel) when is_binary(BinField) ->
    {binary_to_atom(BinModel), binary_to_atom(BinField)};
get_model_and_field({BinModel, AtomField}, _DefaultModel) when is_atom(AtomField) ->
    {binary_to_atom(BinModel), AtomField};
get_model_and_field(BinField, DefaultModel) when is_binary(BinField) ->
    {DefaultModel, binary_to_atom(BinField)};
get_model_and_field(AtomField, DefaultModel) when is_atom(AtomField) ->
    {DefaultModel, AtomField}.

field_alias(Link, MF, Pos, #state{acc=Acc, model=DefaultModel} = State, Next) ->
    {Model, Field} = get_model_and_field(MF, DefaultModel),
    TableLink = link_to_sql(Link, Model),
    case Model of
        undefined ->
            {error, {wrong_format, {Pos, "Unknown model"}}};
        _ ->
            State2 =
                case Field of
                    '*' ->
                        Fields = Model:'$meta'({db_fields, r}),
                        <<$,, FieldsAliases/binary>> =
                            << <<$,, TableLink/binary,
                                 (Model:'$meta'({db_alias, F}))/binary>>
                               || F <- Fields >>,
                        State#state{
                          acc = <<Acc/binary, FieldsAliases/binary>>
                         };
                    _ ->
                        Alias = Model:'$meta'({db_alias, Field}),
                        State#state{
                          acc = <<Acc/binary, TableLink/binary, Alias/binary>>
                         }
                end,
            Next(State2)
    end.

field_query(Link, '...', _Expr, _Pos,
            #state{acc=Acc, fields=Fields, model=Model}=State, Next) ->
    DbFields = Model:'$meta'({db_fields, r}),
    ResultFields = DbFields -- Fields,
    TableLink = link_to_sql(Link, Model),
    FieldsSql = join_fields(TableLink, Model, ResultFields),
    State2 = State#state{
               acc = <<Acc/binary, FieldsSql/binary>>,
               fields = Fields ++ ResultFields
              },
    Next(State2);
field_query(Link, '*', _IsAlias, _Pos,
            #state{acc=Acc, fields=Fields, model=Model}=State, Next) ->
    DbFields = Model:'$meta'({db_fields, r}),
    TableLink = link_to_sql(Link, Model),
    FieldsSql = join_fields(TableLink, Model, DbFields),
    State2 = State#state{
               acc = <<Acc/binary, FieldsSql/binary>>,
               fields = Fields ++ DbFields
              },
    Next(State2);
field_query(Link, {BinModel, BinField}, IsAlias, Pos,
            #state{model=Model}=State, Next) ->
    M = binary_to_atom(BinModel),
    case M of
        Model ->
            TableLink = link_to_sql(Link, M),
            field_query(TableLink, BinField, IsAlias, Pos, State, Next);
        _ ->
            {error, {wrong_model, {Pos, "Querying data from another model not supported"}}}
    end;
field_query(Link, BinField, IsAlias, _Pos,
            #state{acc=Acc, fields=Fields, model=Model}=State, Next)
  when is_binary(BinField) ->
    Field = binary_to_atom(BinField),
    State2 =
        case IsAlias of
            true ->
                TableLink = link_to_sql(Link, Model),
                FieldSql = field_to_sql(TableLink, Model, Field),
                State#state{
                  acc = <<Acc/binary, FieldSql/binary>>
                 };
            false ->
                State
        end,
    State3 = State2#state{
               fields = Fields ++ [Field]
              },
    Next(State3).

arg_type(TypeWrapper, #state{acc=Acc, args=Args, arg_index=ArgIndex} = State, Next) ->
    NewArgIndex = ArgIndex + 1,
    BinArgIndex = integer_to_binary(NewArgIndex),
    State2 =
        State#state{
          acc = <<Acc/binary, $$, BinArgIndex/binary>>,
          args = [TypeWrapper|Args],
          arg_index = NewArgIndex
         },
    Next(State2).

join_fields(_TableLink, _Model, []) ->
    <<>>;
join_fields(TableLink, Model, Fields) ->
    <<$,, Str/binary>> = << <<$,, (field_to_sql(TableLink, Model, F))/binary>> || F <- Fields >>,
    Str.

link_to_sql(none, _Model) -> <<>>;
link_to_sql(table, Model) -> <<(Model:'$meta'(table))/binary, $.>>;
link_to_sql({_Pos, Link}, _Model) -> <<$", Link/binary, $", $.>>.

field_to_sql(TableLink, Model, Field) ->
    <<TableLink/binary, (Model:'$meta'({db_alias, Field}))/binary>>.

binary_to_atom(Bin) ->
    List = binary_to_list(Bin),
    list_to_existing_atom(List).
