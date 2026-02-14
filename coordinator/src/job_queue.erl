-module(job_queue).
-behaviour(gen_server).

-export([start_link/0, enqueue/1, get_pending/0, mark_scheduled/2, mark_completed/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(job, {
    id :: binary(),
    payload :: binary(),
    priority :: integer(),
    max_retries :: integer(),
    timeout_ms :: integer(),
    submitted_at :: binary(),
    retry_count = 0 :: integer(),
    status :: pending | scheduled | running | completed | failed,
    assigned_worker :: binary() | undefined
}).

-record(state, {
    jobs = #{} :: #{binary() => #job{}},
    pending_queue = [] :: [binary()]
}).

%%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

enqueue(JobSpec) ->
    gen_server:call(?MODULE, {enqueue, JobSpec}).

get_pending() ->
    gen_server:call(?MODULE, get_pending).

mark_scheduled(JobId, WorkerId) ->
    gen_server:call(?MODULE, {mark_scheduled, JobId, WorkerId}).

mark_completed(JobId, Result) ->
    gen_server:call(?MODULE, {mark_completed, JobId, Result}).

%%% Callbacks

init([]) ->
    io:format("Job queue started~n"),
    {ok, #state{}}.

handle_call({enqueue, JobSpec}, _From, State) ->
    JobId = maps:get(<<"job_id">>, JobSpec),
    
    Job = #job{
        id = JobId,
        payload = maps:get(<<"payload">>, JobSpec),
        priority = maps:get(<<"priority">>, JobSpec, 5),
        max_retries = maps:get(<<"max_retries">>, JobSpec, 3),
        timeout_ms = maps:get(<<"timeout_ms">>, JobSpec, 30000),
        submitted_at = maps:get(<<"submitted_at">>, JobSpec),
        status = pending
    },
    
    %% Add to jobs map
    NewJobs = maps:put(JobId, Job, State#state.jobs),
    
    %% Add to pending queue (sorted by priority)
    NewPending = insert_by_priority(JobId, Job#job.priority, State#state.pending_queue, NewJobs),
    
    %% Log event
    event_logger:log_event(job_submitted, #{
        job_id => JobId,
        priority => Job#job.priority
    }),
    
    {reply, {ok, JobId}, State#state{jobs = NewJobs, pending_queue = NewPending}};

handle_call(get_pending, _From, State) ->
    %% Return pending jobs as list of maps
    PendingJobs = [job_to_map(maps:get(JobId, State#state.jobs))
                   || JobId <- State#state.pending_queue],
    {reply, PendingJobs, State};

handle_call({mark_scheduled, JobId, WorkerId}, _From, State) ->
    case maps:find(JobId, State#state.jobs) of
        {ok, Job} ->
            UpdatedJob = Job#job{
                status = scheduled,
                assigned_worker = WorkerId
            },
            NewJobs = maps:put(JobId, UpdatedJob, State#state.jobs),
            
            %% Remove from pending queue
            NewPending = lists:delete(JobId, State#state.pending_queue),
            
            event_logger:log_event(job_scheduled, #{
                job_id => JobId,
                worker_id => WorkerId
            }),
            
            {reply, ok, State#state{jobs = NewJobs, pending_queue = NewPending}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({mark_completed, JobId, Result}, _From, State) ->
    case maps:find(JobId, State#state.jobs) of
        {ok, Job} ->
            Status = maps:get(<<"status">>, Result),
            
            %% Check if we need to retry
            ShouldRetry = (Status =/= <<"success">>) andalso 
                         (Job#job.retry_count < Job#job.max_retries),
            
            if
                ShouldRetry ->
                    %% Requeue with incremented retry count
                    UpdatedJob = Job#job{
                        retry_count = Job#job.retry_count + 1,
                        status = pending,
                        assigned_worker = undefined
                    },
                    NewJobs = maps:put(JobId, UpdatedJob, State#state.jobs),
                    NewPending = insert_by_priority(JobId, Job#job.priority, 
                                                   State#state.pending_queue, NewJobs),
                    
                    event_logger:log_event(job_retried, #{
                        job_id => JobId,
                        retry_count => UpdatedJob#job.retry_count,
                        reason => Status
                    }),
                    
                    {reply, {retry, UpdatedJob#job.retry_count}, 
                     State#state{jobs = NewJobs, pending_queue = NewPending}};
                true ->
                    %% Mark as completed
                    FinalStatus = case Status of
                        <<"success">> -> completed;
                        _ -> failed
                    end,
                    
                    UpdatedJob = Job#job{status = FinalStatus},
                    NewJobs = maps:put(JobId, UpdatedJob, State#state.jobs),
                    
                    event_logger:log_event(job_completed, #{
                        job_id => JobId,
                        status => Status,
                        exit_code => maps:get(<<"exit_code">>, Result, -1)
                    }),
                    
                    {reply, ok, State#state{jobs = NewJobs}}
            end;
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

insert_by_priority(JobId, Priority, Queue, Jobs) ->
    %% Insert maintaining priority order (higher priority first)
    insert_sorted(JobId, Priority, Queue, Jobs).

insert_sorted(JobId, Priority, [], _Jobs) ->
    [JobId];
insert_sorted(JobId, Priority, [H | T], Jobs) ->
    case maps:get(H, Jobs) of
        #job{priority = HPriority} when Priority > HPriority ->
            [JobId, H | T];
        _ ->
            [H | insert_sorted(JobId, Priority, T, Jobs)]
    end.

job_to_map(#job{} = Job) ->
    #{
        <<"job_id">> => Job#job.id,
        <<"priority">> => Job#job.priority,
        <<"retry_count">> => Job#job.retry_count,
        <<"submitted_at">> => Job#job.submitted_at,
        <<"status">> => atom_to_binary(Job#job.status, utf8)
    }.
