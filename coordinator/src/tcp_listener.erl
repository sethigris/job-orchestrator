-module(tcp_listener).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    listen_socket :: gen_tcp:socket(),
    port :: integer()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, Port} = application:get_env(coordinator, tcp_port),
    
    {ok, ListenSocket} = gen_tcp:listen(Port, [
        binary,
        {packet, 0},
        {active, false},
        {reuseaddr, true}
    ]),
    
    io:format("TCP listener started on port ~p~n", [Port]),
    
    %% Start accepting connections
    spawn_link(fun() -> accept_loop(ListenSocket) end),
    
    {ok, #state{listen_socket = ListenSocket, port = Port}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    gen_tcp:close(State#state.listen_socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

accept_loop(ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            io:format("New connection accepted~n"),
            %% Spawn handler for this connection
            spawn(fun() -> handle_connection(Socket) end),
            accept_loop(ListenSocket);
        {error, Reason} ->
            io:format("Accept error: ~p~n", [Reason]),
            timer:sleep(1000),
            accept_loop(ListenSocket)
    end.

handle_connection(Socket) ->
    case read_message(Socket) of
        {ok, Message} ->
            case jsx:decode(Message, [return_maps]) of
                #{<<"type">> := <<"register_worker">>} = Msg ->
                    handle_worker_registration(Socket, Msg),
                    worker_loop(Socket, maps:get(<<"worker_id">>, Msg));
                #{<<"type">> := <<"register_job">>} = Msg ->
                    handle_job_submission(Socket, Msg),
                    gen_tcp:close(Socket);
                _ ->
                    io:format("Unknown message type~n"),
                    gen_tcp:close(Socket)
            end;
        {error, Reason} ->
            io:format("Read error: ~p~n", [Reason]),
            gen_tcp:close(Socket)
    end.

handle_worker_registration(Socket, Msg) ->
    WorkerId = maps:get(<<"worker_id">>, Msg),
    MaxConcurrent = maps:get(<<"max_concurrent">>, Msg),
    
    worker_registry:register_worker(WorkerId, Socket, MaxConcurrent),
    
    Response = #{
        <<"type">> => <<"worker_registered">>,
        <<"worker_id">> => WorkerId,
        <<"coordinator_id">> => atom_to_binary(node(), utf8)
    },
    
    send_message(Socket, jsx:encode(Response)).

handle_job_submission(_Socket, Msg) ->
    JobId = maps:get(<<"job_id">>, Msg),
    job_queue:enqueue(Msg),
    io:format("Job submitted: ~s~n", [JobId]).

worker_loop(Socket, WorkerId) ->
    case read_message(Socket) of
        {ok, Message} ->
            case jsx:decode(Message, [return_maps]) of
                #{<<"type">> := <<"heartbeat">>} = Hb ->
                    worker_registry:heartbeat(WorkerId, Hb),
                    worker_loop(Socket, WorkerId);
                #{<<"type">> := <<"job_result">>} = Result ->
                    handle_job_result(Result),
                    worker_loop(Socket, WorkerId);
                _ ->
                    worker_loop(Socket, WorkerId)
            end;
        {error, _Reason} ->
            worker_registry:unregister_worker(WorkerId),
            gen_tcp:close(Socket)
    end.

handle_job_result(Result) ->
    JobId = maps:get(<<"job_id">>, Result),
    job_queue:mark_completed(JobId, Result).

read_message(Socket) ->
    %% Read 4-byte length prefix
    case gen_tcp:recv(Socket, 4) of
        {ok, <<Length:32/big>>} ->
            %% Read message body
            gen_tcp:recv(Socket, Length);
        Error ->
            Error
    end.

send_message(Socket, Message) ->
    Length = byte_size(Message),
    gen_tcp:send(Socket, <<Length:32/big, Message/binary>>).
