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

%% API
-export([
         start_link/1,
         'query'/4,
         'squery'/3,
         transaction/2
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

transaction(Worker, Fun) ->
    gen_server:call(Worker, {'transaction', Fun}).
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

handle_call({'query', Sql, EscapedArgs, Constructor}, _From, #state{conn=Conn} = State) ->
    Resp = tq_postgres_driver_db_query:query(Conn, Sql, EscapedArgs, Constructor),
    {reply, Resp, State};

handle_call({'squery', Sql, Constructor},_From, #state{conn=Conn} = State) ->
    Resp = tq_postgres_driver_db_query:squery(Conn, Sql, Constructor),
    {reply, Resp, State};

handle_call({'transaction', F},_From, #state{conn=Conn} = State) ->
    Resp = tq_postgres_driver_db_query:transaction(Conn, F),
    {reply, Resp, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

