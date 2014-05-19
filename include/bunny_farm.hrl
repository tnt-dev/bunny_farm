-ifndef(BUNNY_FARM_HRL).
-define(BUNNY_FARM_HRL, true).

-include_lib("amqp_client/include/amqp_client.hrl").

%% This is only used for publishing messages. It gets converted to
%% an AMQP message when sending over the wire.
-record(message, {payload, props=[], encoding=bson }).

-type exchange() :: binary().
-type routing_key() :: binary().
-type myabe_binary() :: binary() | {binary(),list()}.

-endif.
