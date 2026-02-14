-module(coordinator_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    
    %% Child specifications
    ChildSpecs = [
        %% Event logger (immutable event log)
        #{
            id => event_logger,
            start => {event_logger, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [event_logger]
        },
        
        %% Job queue manager
        #{
            id => job_queue,
            start => {job_queue, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [job_queue]
        },
        
        %% Worker registry
        #{
            id => worker_registry,
            start => {worker_registry, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [worker_registry]
        },
        
        %% Leader election service
        #{
            id => leader_election,
            start => {leader_election, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [leader_election]
        },
        
        %% Scheduler client
        #{
            id => scheduler_client,
            start => {scheduler_client, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [scheduler_client]
        },
        
        %% TCP listener (supervised separately)
        #{
            id => tcp_listener,
            start => {tcp_listener, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [tcp_listener]
        }
    ],
    
    {ok, {SupFlags, ChildSpecs}}.
