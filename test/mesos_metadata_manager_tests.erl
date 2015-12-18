-module(mesos_metadata_manager_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_ZK_SERVER, [{"localhost", 2181}]).

md_test_() ->
    SetupFun = fun() ->
                       application:ensure_all_started(erlzk),
                       (catch mesos_metadata_manager:stop()),
                       {ok, _Pid} = mesos_metadata_manager:start_link(?TEST_ZK_SERVER,"md-mgr-test")
               end,
    TeardownFun = fun(_) -> ok end,

    {setup,
     SetupFun,
     TeardownFun,
     [
      fun create_delete/0
     ]}.

create_delete() ->
    {ok, RootName, _Data = <<>>} = mesos_metadata_manager:get_root_node(),
    ChildName = RootName ++ "/child",

    %% Make sure there's nothing left over from previous tests
    mesos_metadata_manager:recursive_delete(ChildName),
    ?assertEqual({error, no_node}, mesos_metadata_manager:get_node(ChildName)),

    ?assertEqual({ok, ChildName, _Data = <<>>},
                 mesos_metadata_manager:make_empty_child(RootName, "child")),
    ?assertEqual({ok, ChildName, <<>>}, mesos_metadata_manager:get_node(ChildName)),

    ok = mesos_metadata_manager:delete_children(RootName),
    ?assertEqual({error, no_node}, mesos_metadata_manager:get_node(ChildName)),

    pass.
