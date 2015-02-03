%%%-------------------------------------------------------------------
%%% @author isergey
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Feb 2015 1:05 PM
%%%-------------------------------------------------------------------
-module(tq_postgres_driver_utils).
-author("isergey").

%% API
-export([escape_args/1]).

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
            catch _:_ ->
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
            catch _:_ ->
                try
                    {ok, binary_to_float(Bin)}
                catch _:_ ->
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
escape_arg({date, Arg}) ->
    case Arg of
        {_Y, _M, _D} ->
            {ok, Arg};
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
    {error, {type_mismatch, lists:reverse(Errors)}};
error_writer_map(Fun, [{Type, Value}=H|T], Acc, Errors) ->
    case Fun(H) of
        {ok, R} ->
            error_writer_map(Fun, T, [R|Acc], Errors);
        {error, R} ->
            error_writer_map(Fun, T, Acc, [{R, [Type, Value]}|Errors])
    end.
