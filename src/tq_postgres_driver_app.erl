-module(tq_postgres_driver_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    tq_postgres_driver_sup:start_link().

stop(_State) ->
    ok.
