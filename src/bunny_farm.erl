%% Copyright 2011 Brian Lee Yung Rowe
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(bunny_farm).
-behaviour(gen_server).

-include("bunny_farm.hrl").
-include("private_macros.hrl").
-compile([{parse_transform, lager_transform}]).
-export([open/1, open/2, close/1, close/2]).
-export([consume/1, consume/2,
         publish/3,
         rpc/3, rpc/4, respond/3]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).
-ifdef(TEST).
-compile(export_all).
-endif.

-define(RECONNECT_TIMEOUT, 5000).

-record(state, {spec, queue, exchange, options,
                connection, channel, connection_ref, channel_ref,
                consumes=[]}).

%% Convenience function for opening a connection for publishing
%% messages. The routing key can be included but if it is not,
%% then the connection can be re-used for multiple routing keys
%% on the same exchange.
%% Example
%%   BusHandle = bunny_farm:open(<<"my.exchange">>),
%%   bunny_farm:publish(Message, K,BusHandle),
open(MaybeTuple) ->
  open_it({publish, MaybeTuple}).

%% Convenience function to open and declare all intermediate objects. This
%% is the typical pattern for consuming messages from a topic exchange.
%% @returns bus_handle
%% Example
%%   BusHandle = bunny_farm:open(X, K),
%%   bunny_farm:consume(BusHandle),
open(MaybeX, MaybeK) ->
    open_it({consume, {MaybeX, MaybeK}}).

close(Pid) ->
    close(Pid, <<"">>).
close(Pid, Tag) ->
    gen_server:cast(Pid, {close, Tag}).

consume(Pid) ->
    consume(Pid, []).
consume(Pid, Options) when is_list(Options) ->
    gen_server:call(Pid, {consume, Options, self()}).

%publish(#message{}=Message, #bus_handle{}=BusHandle) ->
%  publish(Message, BusHandle#bus_handle.routing_key, BusHandle);

%publish(Payload, #bus_handle{}=BusHandle) ->
%  publish(#message{payload=Payload}, BusHandle).

%% This is the recommended call to use as the same exchange can be reused
publish(#message{}=Message, RoutingKey, Pid) ->
    gen_server:cast(Pid, {publish, Message, RoutingKey});
publish(Payload, RoutingKey, Pid) ->
    publish(#message{payload=Payload}, RoutingKey, Pid).

rpc(#message{}=Message, RoutingKey, Pid) ->
    gen_server:cast(Pid, {rpc, Message, RoutingKey}).

rpc(Payload, ReplyTo, K, Pid) ->
    Props = [{reply_to, ReplyTo}, {correlation_id, ReplyTo}],
    rpc(#message{payload=Payload, props=Props}, K, Pid).

%% This is used to send the response of an RPC. The primary difference
%% between this and publish is that the data is retained as an erlang
%% binary.
respond(#message{}=Message, RoutingKey, Pid) ->
    gen_server:cast(Pid, {respond, Message, RoutingKey});
respond(Payload, RoutingKey, Pid) ->
    respond(#message{payload=Payload}, RoutingKey, Pid).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Spec]) ->
    {ok, State1} = connect(#state{spec=Spec}),
    State2 = declare(Spec, State1),
    {ok, State2}.

handle_call({consume, Options, Pid}, _From, #state{queue=Q,
                                                   channel=Channel,
                                                   consumes=Consumes}=State) ->
    AllOptions = [{no_ack,true} | Options],
    Reply = subscribe(Q, AllOptions, Pid, Channel),
    {reply, Reply, State#state{consumes=[{AllOptions, Pid} | Consumes]}};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({publish, Message, K}, #state{exchange=X,
                                          options=Options,
                                          channel=Channel}=State) ->
    #message{payload=Payload, props=Props} = Message,
    MimeType = case farm_tools:content_type(Props) of
                 undefined -> farm_tools:encoding(Options);
                 M -> M
             end,
    EncPayload = farm_tools:encode_payload(MimeType, Payload),
    ContentType = {content_type, MimeType},
    AProps = farm_tools:to_amqp_props(lists:merge([ContentType], Props)),
    AMsg = #amqp_msg{payload=EncPayload, props=AProps},
    BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
    amqp_channel:cast(Channel, BasicPublish, AMsg),
    {noreply, State};
handle_cast({rpc, Message, K}, #state{exchange=X,
                                      options=Options,
                                      channel=Channel}=State) ->
    #message{payload=Payload, props=Props} = Message,
    MimeType = case farm_tools:content_type(Props) of
                   undefined -> proplists:get_value(encoding, Options);
                   M -> M
               end,
    ContentType = {content_type,MimeType},
    AProps = farm_tools:to_amqp_props(lists:merge([ContentType], Props)),
    AMsg = #amqp_msg{payload=farm_tools:encode_payload(MimeType, Payload),
                     props=AProps},
    BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
    amqp_channel:cast(Channel, BasicPublish, AMsg),
    {noreply, State};
handle_cast({respond, Message, K}, #state{exchange=X,
                                          channel=Channel}=State) ->
    #message{payload=Payload, props=Props} = Message,
    MimeType = farm_tools:content_type(Props),
    AMsg = #amqp_msg{payload=farm_tools:encode_payload(MimeType, Payload),
                     props=farm_tools:to_amqp_props(Props)},
    BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
    amqp_channel:cast(Channel, BasicPublish, AMsg),
    {noreply, State};
handle_cast({close, ChannelTag}, #state{channel=Channel,
                                        connection_ref=ConnectionRef,
                                        channel_ref=ChannelRef}=State) ->
    case ChannelTag of
        <<"">> -> ok;
        Tag ->
            #'basic.cancel_ok'{} =
                amqp_channel:call(Channel, #'basic.cancel'{consumer_tag=Tag})
    end,
    erlang:demonitor(ChannelRef),
    erlang:demonitor(ConnectionRef),
    close_connection(State),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(reconnect, #state{spec=Spec, consumes=Consumes}=State) ->
  case connect(State) of
      {ok, #state{channel=Channel}=NewState} ->
          NewState2 = #state{queue=Q} = declare(Spec, NewState),
          [subscribe(Q, Options, Pid, Channel) || {Options, Pid} <- Consumes],
          {noreply, NewState2};
      {error, _} ->
          erlang:send_after(?RECONNECT_TIMEOUT, self(), reconnect),
          {noreply, State}
  end;
handle_info({'DOWN', ConnectionRef, process, Connection, Reason},
            #state{exchange=Exchange,
                   options=Options,
                   connection=Connection,
                   connection_ref=ConnectionRef,
                   channel=Channel}=State) ->
    lager:error("AMQP connection error (~p ~p ~p ~p): ~p",
                [Connection, Channel, Exchange, Options, Reason]),
    erlang:send_after(?RECONNECT_TIMEOUT, self(), reconnect),
    {noreply, State#state{connection=undefined, channel=undefined}};
handle_info({'DOWN', ChannelRef, process, Channel, Reason},
            #state{exchange=Exchange,
                   options=Options,
                   connection=Connection,
                   channel=Channel,
                   channel_ref=ChannelRef}=State) ->
    lager:error("AMQP channel error (~p ~p ~p ~p): ~p",
                [Connection, Channel, Exchange, Options, Reason]),
    case is_pid(Connection) of
        true  -> amqp_connection:close(Connection);
        false -> ok
    end,
    erlang:send_after(?RECONNECT_TIMEOUT, self(), reconnect),
    {noreply, State#state{connection=undefined, channel=undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    close_connection(State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open_it(Options) ->
    {ok, ConnPid} = gen_server:start(?MODULE, [Options], []),
    ConnPid.

connect(State) ->
    Params = amqp_params(),
    lager:debug("Opening AMQP connection to ~p:~p",
                [Params#amqp_params.host,
                 Params#amqp_params.port]),
    case amqp_connection:start(network, Params) of
        {ok, Connection} ->
            lager:debug("AMQP connection ~p to ~p:~p established",
                        [Connection,
                         Params#amqp_params.host,
                         Params#amqp_params.port]),
            case amqp_connection:open_channel(Connection) of
                {ok, Channel} ->
                    lager:debug("AMQP channel ~p on connection ~p"
                                " to ~p:~p opened",
                                [Channel,
                                 Connection,
                                 Params#amqp_params.host,
                                 Params#amqp_params.port]),
                    ConnectionRef = erlang:monitor(process, Connection),
                    ChannelRef    = erlang:monitor(process, Channel),
                    {ok, State#state{connection=Connection,
                                     channel=Channel,
                                     connection_ref=ConnectionRef,
                                     channel_ref=ChannelRef}};
                {error, Reason}=Error1 ->
                    lager:error("AMQP channel on connection ~p to ~p:~p"
                                " could not be open: ~p",
                                [Connection,
                                 Params#amqp_params.host,
                                 Params#amqp_params.port,
                                 Reason]),
                    amqp_connection:close(Connection),
                    Error1
            end;
        {error, Reason}=Error2 ->
            lager:error("AMQP connection to ~p:~p could not be established: ~p",
                        [Params#amqp_params.host,
                         Params#amqp_params.port,
                         Reason]),
            Error2
    end.

declare({publish, MaybeTuple}, #state{channel=Channel}=State) ->
    {X, XO} = resolve_options(exchange, MaybeTuple),
    declare_exchange(X, XO, Channel),
    State#state{exchange=X, options=XO};
declare({consume, {MaybeX, MaybeK}}, #state{channel=Channel}=State) ->
    {X, XO} = resolve_options(exchange, MaybeX),
    {K, KO} = resolve_options(queue, MaybeK),
    declare_exchange(X, XO, Channel),
    Q = case X of
            <<"">> -> declare_queue(K, KO, Channel);
            _      -> declare_queue(KO, Channel)
        end,
    bind(Q, K, X, Channel),
    State#state{queue=Q, exchange=X, options=XO}.

declare_exchange(<<"">>, _Options, _Channel) ->
    ok;
declare_exchange(Key, Options, Channel) ->
    AllOptions = lists:merge([{exchange, Key}], Options),
    ExchDeclare = farm_tools:to_exchange_declare(AllOptions),
    lager:debug("Declare AMQP exchange ~p on channel ~p",
                [ExchDeclare, Channel]),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchDeclare),
    ok.

%% http://www.rabbitmq.com/amqp-0-9-1-quickref.html
%% Use configured options for the queue. Since no routing key is specified,
%% attempt to read options for the routing key <<"">>.

%% Options - Tuple list of k,v options
declare_queue(Options, Channel) when is_list(Options) ->
    declare_queue(<<"">>, Options, Channel).

declare_queue(Key, Options, Channel) ->
    AllOptions = lists:merge([{queue,Key}], Options),
    QueueDeclare = farm_tools:to_queue_declare(AllOptions),
    lager:debug("Declare AMQP queue ~p on channel ~p",
                [QueueDeclare, Channel]),
    #'queue.declare_ok'{queue=Q} = amqp_channel:call(Channel, QueueDeclare),
    Q.

bind(_Q, _BindKey, <<"">>, _Channel) ->
    ok;
bind(Q, BindKey, X, Channel) ->
    QueueBind = #'queue.bind'{exchange=X, queue=Q, routing_key=BindKey},
    lager:debug("Bind AMQP queue ~p on channel ~p",
                [QueueBind, Channel]),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    ok.

subscribe(Q, Options, Pid, Channel) ->
    BasicConsume = farm_tools:to_basic_consume([{queue, Q} | Options]),
    lager:debug("Sending AMQP subscription request ~p on channel ~p",
                [BasicConsume, Channel]),
    amqp_channel:subscribe(Channel, BasicConsume, Pid).

amqp_params() ->
    Keys = [amqp_username, amqp_password, amqp_virtual_host, amqp_heartbeat],
    {Host, Port} = get_server(),
    [Username, Password, VHost, Heartbeat] = lists:map(fun get_env/1, Keys),
    #amqp_params{username=Username,
                 password=Password,
                 virtual_host=VHost,
                 host=Host,
                 port=Port,
                 heartbeat=Heartbeat}.

default(Key) ->
    Defaults = [{amqp_username, <<"guest">>},
                {amqp_password, <<"guest">>},
                {amqp_virtual_host, <<"/">>},
                {amqp_servers, []}, % Format is {host,port}
                {amqp_host, "localhost"},
                {amqp_port, 5672},
                {amqp_encoding, <<"application/x-erlang">>},
                {amqp_exchanges, []},
                {amqp_queues, []},
                {amqp_heartbeat, 0}],

    %% Note(superbobry): try fetching the value from 'bunny_farm'
    %% environment first, this might be useful for example when all
    %% applications use a single RabbitMQ instance.
    case application:get_env(bunny_farm, Key) of
        {ok, Value} -> Value;
        undefined   ->
            proplists:get_value(Key, Defaults)
    end.

get_env(Key) ->
    Default = default(Key),
    case application:get_env(Key) of
        undefined -> Default;
        {ok, H}   -> H
    end.

%% If amqp_servers is defined, use that. Otherwise fall back to amqp_host and
%% amqp_port
get_server() ->
    case get_env(amqp_servers) of
        [] ->
            {get_env(amqp_host), get_env(amqp_port)};
        Servers ->
            Idx = random:uniform(length(Servers)),
            lists:nth(Idx, Servers)
    end.

%% Define defaults that override rabbitmq defaults
exchange_defaults() ->
    Encoding = get_env(amqp_encoding),
    lists:sort([{encoding, Encoding}, {type,<<"topic">>}]).

%% Get the proplist for a given channel
exchange_options(X) ->
    Channels = get_env(amqp_exchanges),
    ChannelList = [ List || {K, List} <- Channels, K == X ],
    case ChannelList of
        []        -> [];
        [Channel] -> lists:sort(Channel)
    end.

%% Define defaults that override rabbitmq defaults
queue_defaults() ->
    lists:sort([{exclusive,true}]).

queue_options(X) ->
    Channels = get_env(amqp_queues),
    ChannelList = [List || {K, List} <- Channels, K == X],
    case ChannelList of
        []        -> [];
        [Channel] -> lists:sort(Channel)
    end.

%% Decouple the exchange and options. If no options exist, then use defaults.
resolve_options(exchange, MaybeTuple) ->
    Defaults = exchange_defaults(),
    case MaybeTuple of
        {X,O} -> {X, lists:merge([lists:sort(O), exchange_options(X), Defaults])};
        X     -> {X, lists:merge([exchange_options(X), Defaults])}
    end;
resolve_options(queue, MaybeTuple) ->
    Defaults = queue_defaults(),
    case MaybeTuple of
        {K,O} -> {K, lists:merge([lists:sort(O), queue_options(K), Defaults])};
        K     -> {K, lists:merge(queue_options(K), Defaults)}
    end.

close_connection(#state{connection=Connection, channel=Channel}) ->
    case is_pid(Channel) of
        true  -> amqp_channel:close(Channel);
        false -> ok
    end,
    case is_pid(Connection) of
      true  -> amqp_connection:close(Connection);
        false -> ok
    end,
    ok.
