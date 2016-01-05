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
      fun create_delete/0,
      fun create_with_data/0,
      fun set_get/0,
      fun get_children/0,
      fun create_or_set/0
     ]}.

create_delete() ->
    {RootName, ChildName} = reset_test_metadata("child"),

    create_helper(RootName, ChildName, false),
    delete_helper(RootName, ChildName),

    create_helper(RootName, ChildName, true),
    delete_helper(RootName, ChildName),

    pass.

create_with_data() ->
    {RootName, ChildName} = reset_test_metadata("child"),
    Data = <<"riak-testing-123">>,

    ?assertEqual({ok, ChildName, Data},
                 mesos_metadata_manager:make_child_with_data(RootName, "child", Data)),
    ?assertEqual({ok, ChildName, Data},
                 mesos_metadata_manager:get_node(ChildName)),

    pass.

set_get() ->
    {RootName, ChildName} = reset_test_metadata("child"),

    Data1 = <<"first">>,
    Data2 = <<"second">>,

    ?assertEqual({ok, ChildName, Data1},
                 mesos_metadata_manager:make_child_with_data(RootName, "child", Data1)),

    ?assertEqual(ok, mesos_metadata_manager:set_data(ChildName, Data2)),
    ?assertEqual({ok, ChildName, Data2}, mesos_metadata_manager:get_node(ChildName)),

    pass.

get_children() ->
    {RootName, ChildName} = reset_test_metadata("child"),
    create_helper(RootName, ChildName, false),

    %% If we check the children of the root node, we may see results from other tests,
    %% so make everything a child of the base "child" node.
    {ok, _SubChild1, <<>>} = mesos_metadata_manager:make_child(ChildName, "subchild1"),
    {ok, _SubChild2, <<>>} = mesos_metadata_manager:make_child(ChildName, "subchild2"),

    {ok, Result} = mesos_metadata_manager:get_children(ChildName),
    ?assertEqual(["subchild1", "subchild2"], lists:sort(Result)),

    pass.

create_or_set() ->
    {RootName, ChildName} = reset_test_metadata("child"),
    CreateData = <<"test-create">>,
    UpdateData = <<"test-update">>,

    {ok, ChildName, CreateData} = mesos_metadata_manager:create_or_set(
                                    RootName, "child", CreateData),
    {ok, _, CreateData} = mesos_metadata_manager:get_node(ChildName),

    {ok, _, UpdateData} = mesos_metadata_manager:create_or_set(RootName, "child", UpdateData),
    {ok, _, UpdateData} = mesos_metadata_manager:get_node(ChildName),

    pass.

reset_test_metadata(Child) ->
    {ok, RootName, _Data = <<>>} = mesos_metadata_manager:get_root_node(),

    ChildName = RootName ++ "/" ++ Child,
    ?assertEqual(ok, mesos_metadata_manager:recursive_delete(ChildName)),
    ?assertEqual({error, no_node}, mesos_metadata_manager:get_node(ChildName)),

    {RootName, ChildName}.

create_helper(RootName, ChildName, Ephemeral) ->
    ?assertEqual({ok, ChildName, _Data = <<>>},
                 mesos_metadata_manager:make_child(RootName, "child", Ephemeral)),
    ?assertEqual({ok, ChildName, <<>>}, mesos_metadata_manager:get_node(ChildName)).

delete_helper(RootName, ChildName) ->
    ok = mesos_metadata_manager:delete_children(RootName),
    ?assertEqual({error, no_node}, mesos_metadata_manager:get_node(ChildName)).
