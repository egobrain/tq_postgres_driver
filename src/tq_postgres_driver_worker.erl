%%%-------------------------------------------------------------------
%%% @author egobrain <>
%%% @copyright (C) 2013, egobrain
%%% @doc
%%%
%%% @end
%%% Created :  3 Jul 2013 by egobrain <>
%%%-------------------------------------------------------------------
-module(tq_postgres_driver_worker).

-behaviour(gen_server).

-include_lib("epgsql/include/pgsql.hrl").
%% API
-export([
         start_link/1,
         'query'/4,
         'squery'/3
        ]).

%% gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define(SERVER, ?MODULE).

-record(state, {conn}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

'squery'(Worker, Sql, Contructor) ->
    gen_server:call(Worker, {'squery', Sql, Contructor}).

'query'(Worker, Sql, EscapedArgs, Contructor) ->
    gen_server:call(Worker, {'query', Sql, EscapedArgs, Contructor}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Opts),
    Database = proplists:get_value(database, Opts),
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts),
    case pgsql:connect(
           Hostname, Username, Password,
           [
            {database, Database}
           ]) of
        {ok, Conn} ->
            {ok, #state{conn=Conn}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(
  {'query', Sql, EscapedArgs, Constructor},
  _From,
  #state{conn=Conn} = State) ->
    Resp =
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
        end,
    {reply, Resp, State};

handle_call(
    {'squery', Sql, Constructor},
    _From,
    #state{conn=Conn} = State) ->
    Resp =
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

    end,
    {reply, Resp, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
