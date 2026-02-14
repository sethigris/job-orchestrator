-module(worker_registry).
-behaviour(gen_server).

-export([start_link/0, register_worker/3, unregister_worker/1, heartbeat/2, 
         get_available_workers/0, get_worker_info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(HEARTBEAT_TIMEOUT, 15000). %% 15 seconds

-record(worker, {
    id :: binary(),
    socket :: gen_tcp:socket(),
    max_concurrent :: integer(),
    capabilities :: [binary()],
    load :: float(),
    jobs_running :: integer(),
    jobs_completed :: integer(),
    last_heartbeat :: integer(),
    status :: available | busy | dead
}).

-record(state, {
    workers = #{} :: #{binary() => #worker{}},
    timers = #{} :: #{binary() => reference()}
}).

%%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_worker(WorkerId, Socket, MaxConcurrent) ->
    gen_server:call(?MODULE, {register_worker, WorkerId, Socket, MaxConcurrent}).

unregister_worker(WorkerId) ->
    gen_server:cast(?MODULE, {unregister_worker, WorkerId}).

heartbeat(WorkerId, Stats) ->
    gen_server:cast(?MODULE, {heartbeat, WorkerId, Stats}).

get_available_workers() ->
    gen_server:call(?MODULE, get_available_workers).

get_worker_info(WorkerId) ->
    gen_server:call(?MODULE, {get_worker_info, WorkerId}).

%%% Callbacks

init([]) ->
    io:format("Worker registry started~n"),
    {ok, #state{}}.

handle_call({register_worker, WorkerId, Socket, MaxConcurrent}, _From, State) ->
    Worker = #worker{
        id = WorkerId,
        socket = Socket,
        max_concurrent = MaxConcurrent,
        capabilities = [<<"shell">>],
        load = 0.0,
        jobs_running = 0,
        jobs_completed = 0,
        last_heartbeat = erlang:system_time(millisecond),
        status = available
    },
    
    %% Start heartbeat timeout
    TimerRef = erlang:send_after(?HEARTBEAT_TIMEOUT, self(), {heartbeat_timeout, WorkerId}),
    
    NewWorkers = maps:put(WorkerId, Worker, State#state.workers),
    NewTimers = maps:put(WorkerId, TimerRef, State#state.timers),
    
    io:format("Worker registered: ~s~n", [WorkerId]),
    event_logger:log_event(worker_registered, #{worker_id => WorkerId}),
    
    {reply, ok, State#state{workers = NewWorkers, timers = NewTimers}};

handle_call(get_available_workers, _From, State) ->
    Available = maps:fold(
        fun(WorkerId, Worker, Acc) ->
            case Worker#worker.status of
                available -> [WorkerId | Acc];
                _ -> Acc
            end
        end,
        [],
        State#state.workers
    ),
    {reply, Available, State};

handle_call({get_worker_info, WorkerId}, _From, State) ->
    case maps:find(WorkerId, State#state.workers) of
        {ok, Worker} ->
            Info = #{
                id => Worker#worker.id,
                socket => Worker#worker.socket,
                load => Worker#worker.load,
                jobs_running => Worker#worker.jobs_running,
                status => Worker#worker.status
            },
            {reply, {ok, Info}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({unregister_worker, WorkerId}, State) ->
    %% Cancel timer
    case maps:find(WorkerId, State#state.timers) of
        {ok, TimerRef} -> erlang:cancel_timer(TimerRef);
        error -> ok
    end,
    
    NewWorkers = maps:remove(WorkerId, State#state.workers),
    NewTimers = maps:remove(WorkerId, State#state.timers),
    
    io:format("Worker unregistered: ~s~n", [WorkerId]),
    event_logger:log_event(worker_unregistered, #{worker_id => WorkerId}),
    
    {noreply, State#state{workers = NewWorkers, timers = NewTimers}};

handle_cast({heartbeat, WorkerId, Stats}, State) ->
    case maps:find(WorkerId, State#state.workers) of
        {ok, Worker} ->
            %% Update worker stats
            UpdatedWorker = Worker#worker{
                load = maps:get(<<"load">>, Stats, 0.0),
                jobs_running = maps:get(<<"jobs_running">>, Stats, 0),
                jobs_completed = maps:get(<<"jobs_completed">>, Stats, 0),
                last_heartbeat = erlang:system_time(millisecond),
                status = case maps:get(<<"load">>, Stats, 0.0) of
                    L when L >= 1.0 -> busy;
                    _ -> available
                end
            },
            
            %% Reset heartbeat timeout
            OldTimer = maps:get(WorkerId, State#state.timers),
            erlang:cancel_timer(OldTimer),
            NewTimer = erlang:send_after(?HEARTBEAT_TIMEOUT, self(), {heartbeat_timeout, WorkerId}),
            
            NewWorkers = maps:put(WorkerId, UpdatedWorker, State#state.workers),
            NewTimers = maps:put(WorkerId, NewTimer, State#state.timers),
            
            {noreply, State#state{workers = NewWorkers, timers = NewTimers}};
        error ->
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({heartbeat_timeout, WorkerId}, State) ->
    io:format("Worker heartbeat timeout: ~s~n", [WorkerId]),
    
    case maps:find(WorkerId, State#state.workers) of
        {ok, Worker} ->
            %% Mark worker as dead
            DeadWorker = Worker#worker{status = dead},
            NewWorkers = maps:put(WorkerId, DeadWorker, State#state.workers),
            
            event_logger:log_event(worker_failed, #{
                worker_id => WorkerId,
                reason => heartbeat_timeout
            }),
            
            %% TODO: Reschedule jobs that were assigned to this worker
            
            {noreply, State#state{workers = NewWorkers}};
        error ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
