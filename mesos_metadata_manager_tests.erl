-module(mesos_metadata_manager_tests).

-include_lib("eunit/include/eunit.hrl").

-export([run/1]).

run(Server) ->
    SetupFun = fun() ->
                       {ok, _Pid} = mesos_metadata_manager:start_link(Server, <<"md-mgr-test">>)
               end,
    TeardownFun = fun(_) -> mesos_metadata_manager:stop() end,

    Tests = {setup,
             SetupFun,
             TeardownFun,
             [
              fun dummy/0
             ]},

    eunit:test(Tests).

dummy() ->
    pass.
