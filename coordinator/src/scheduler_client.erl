-module(scheduler_client).
-behaviour(gen_server).

-export([start_link/0, request_schedule/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SCHEDULE_INTERVAL, 2000). %% Schedule every 2 seconds

-record(state, {
    scheduler_host :: string(),
    scheduler_port :: integer()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

request_schedule() ->
    gen_server:cast(?MODULE, request_schedule).

init([]) ->
    {ok, Host} = application:get_env(coordinator, scheduler_host),
    {ok, Port} = application:get_env(coordinator, scheduler_port),
    
    io:format("Scheduler client started (scheduler at ~s:~p)~n", [Host, Port]),
    
    %% Start periodic scheduling
    erlang:send_after(?SCHEDULE_INTERVAL, self(), schedule_tick),
    
    {ok, #state{scheduler_host = Host, scheduler_port = Port}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(request_schedule, State) ->
    do_schedule(State),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(schedule_tick, State) ->
    %% Only schedule if we're the leader
    case leader_election:am_i_leader() of
        true -> do_schedule(State);
        false -> ok
    end,
    
    erlang:send_after(?SCHEDULE_INTERVAL, self(), schedule_tick),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

do_schedule(State) ->
    %% Get pending jobs
    PendingJobs = job_queue:get_pending(),
    
    %% Get available workers
    AvailableWorkers = worker_registry:get_available_workers(),
    
    if
        length(PendingJobs) =:= 0 ->
            ok; %% No jobs to schedule
        length(AvailableWorkers) =:= 0 ->
            ok; %% No workers available
        true ->
            %% Call Haskell scheduler
            Request = #{
                <<"type">> => <<"schedule_request">>,
                <<"pending_jobs">> => PendingJobs,
                <<"available_workers">> => AvailableWorkers
            },
            
            %% For now, simple round-robin scheduling (Haskell scheduler optional)
            Assignments = simple_schedule(PendingJobs, AvailableWorkers),
            
            %% Execute assignments
            lists:foreach(
                fun(#{<<"job_id">> := JobId, <<"worker_id">> := WorkerId}) ->
                    assign_job(JobId, WorkerId)
                end,
                Assignments
            )
    end.

simple_schedule(Jobs, Workers) ->
    simple_schedule(Jobs, Workers, Workers, []).

simple_schedule([], _, _, Acc) ->
    lists:reverse(Acc);
simple_schedule(_, [], _, Acc) ->
    lists:reverse(Acc);
simple_schedule([Job | RestJobs], [], OriginalWorkers, Acc) ->
    %% Restart worker list
    simple_schedule([Job | RestJobs], OriginalWorkers, OriginalWorkers, Acc);
simple_schedule([Job | RestJobs], [Worker | RestWorkers], OriginalWorkers, Acc) ->
    JobId = maps:get(<<"job_id">>, Job),
    Assignment = #{
        <<"job_id">> => JobId,
        <<"worker_id">> => Worker
    },
    simple_schedule(RestJobs, RestWorkers, OriginalWorkers, [Assignment | Acc]).

assign_job(JobId, WorkerId) ->
    %% Mark job as scheduled
    job_queue:mark_scheduled(JobId, WorkerId),
    
    %% Get worker socket
    case worker_registry:get_worker_info(WorkerId) of
        {ok, #{socket := Socket}} ->
            %% Get job details
            %% Send execute command to worker
            io:format("Assigned job ~s to worker ~s~n", [JobId, WorkerId]),
            %% TODO: Actually send the job to the worker
            ok;
        {error, not_found} ->
            io:format("Worker ~s not found~n", [WorkerId])
    end.
