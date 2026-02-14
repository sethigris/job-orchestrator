-module(coordinator_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    io:format("~n=== Starting Job Coordinator ===~n"),
    io:format("Node: ~p~n", [node()]),
    io:format("Cookie: ~p~n~n", [erlang:get_cookie()]),
    
    %% Start the supervisor
    case coordinator_sup:start_link() of
        {ok, Pid} ->
            io:format("Coordinator started successfully~n"),
            {ok, Pid};
        Error ->
            io:format("Failed to start coordinator: ~p~n", [Error]),
            Error
    end.

stop(_State) ->
    io:format("~nStopping coordinator~n"),
    ok.
