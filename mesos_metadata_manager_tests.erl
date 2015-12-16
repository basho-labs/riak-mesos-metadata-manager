-module(mesos_metadata_manager_tests).

-include_lib("eunit/include/eunit.hrl").

-export([run/1]).

run(Server) ->
    SetupFun = fun() ->
                       {ok, _Pid} = mesos_metadata_manager:start_link(Server, "md-mgr-test")
               end,
    TeardownFun = fun(_) -> mesos_metadata_manager:stop() end,

    Tests = {setup,
             SetupFun,
             TeardownFun,
             [
              fun create_delete/0
             ]},

    eunit:test(Tests).

-define(CHILD_NODE, "child").

create_delete() ->
    {ok, RootName, _Data = <<>>} = mesos_metadata_manager:get_root_node(),
    ?assertEqual({error, no_node}, mesos_metadata_manager:get_node(?CHILD_NODE)),

    {ok, ChildNode, _Data = <<>>} = mesos_metadata_manager:make_empty_child(RootName, ?CHILD_NODE),
    ?assertEqual({ok, ChildNode, <<>>}, mesos_metadata_manager:get_node(?CHILD_NODE)),

    ok = mesos_metadata_manager:delete_children(RootName),
    ?assertEqual({error, no_node}, mesos_metadata_manager:get_node(?CHILD_NODE)),

    pass.
