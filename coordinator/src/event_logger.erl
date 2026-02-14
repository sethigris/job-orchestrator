-module(event_logger).
-behaviour(gen_server).

-export([start_link/0, log_event/2, get_events/1, get_all_events/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    events = [] :: list(),
    log_file :: file:io_device() | undefined
}).

%%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

log_event(EventType, Metadata) ->
    gen_server:cast(?MODULE, {log_event, EventType, Metadata}).

get_events(JobId) ->
    gen_server:call(?MODULE, {get_events, JobId}).

get_all_events() ->
    gen_server:call(?MODULE, get_all_events).

%%% Callbacks

init([]) ->
    %% Open log file for append-only writes
    LogPath = "coordinator_events.log",
    {ok, LogFile} = file:open(LogPath, [append, raw, binary]),
    io:format("Event logger started, logging to ~s~n", [LogPath]),
    {ok, #state{log_file = LogFile}}.

handle_call({get_events, JobId}, _From, State) ->
    Events = lists:filter(
        fun(Event) ->
            maps:get(job_id, Event, undefined) =:= JobId
        end,
        State#state.events
    ),
    {reply, Events, State};

handle_call(get_all_events, _From, State) ->
    {reply, lists:reverse(State#state.events), State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({log_event, EventType, Metadata}, State) ->
    Event = #{
        timestamp => iso8601_timestamp(),
        event_type => EventType,
        metadata => Metadata
    },
    
    %% Write to file (immutable, append-only)
    LogLine = jsx:encode(Event),
    ok = file:write(State#state.log_file, [LogLine, $\n]),
    ok = file:sync(State#state.log_file),
    
    %% Keep in memory for fast queries
    NewEvents = [Event | State#state.events],
    
    io:format("[EVENT] ~s: ~p~n", [EventType, Metadata]),
    
    {noreply, State#state{events = NewEvents}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.log_file of
        undefined -> ok;
        LogFile -> file:close(LogFile)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

iso8601_timestamp() ->
    {{Y, M, D}, {H, Min, S}} = calendar:universal_time(),
    io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ",
                  [Y, M, D, H, Min, S]).
