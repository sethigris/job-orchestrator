-module(leader_election).
-behaviour(gen_server).

-export([start_link/0, am_i_leader/0, get_leader/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    leader :: node() | undefined,
    is_leader = false :: boolean()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

am_i_leader() ->
    gen_server:call(?MODULE, am_i_leader).

get_leader() ->
    gen_server:call(?MODULE, get_leader).

init([]) ->
    %% Simple leader election: lowest node name wins
    erlang:send_after(1000, self(), check_leader),
    {ok, #state{}}.

handle_call(am_i_leader, _From, State) ->
    {reply, State#state.is_leader, State};

handle_call(get_leader, _From, State) ->
    {reply, State#state.leader, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_leader, State) ->
    Nodes = lists:sort([node() | nodes()]),
    Leader = hd(Nodes),
    IsLeader = Leader =:= node(),
    
    case {State#state.is_leader, IsLeader} of
        {false, true} ->
            io:format("~nI am now the leader!~n~n"),
            event_logger:log_event(leader_elected, #{node => node()});
        {true, false} ->
            io:format("~nI am no longer the leader~n~n");
        _ ->
            ok
    end,
    
    erlang:send_after(5000, self(), check_leader),
    {noreply, State#state{leader = Leader, is_leader = IsLeader}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
