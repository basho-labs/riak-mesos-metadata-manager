-module(riak_mesos_md_mgr_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    mesos_metadata_manager:start_link([{"localhost", 2181}], "riak_mesos_md_mgr").

stop(_State) ->
    ok.
