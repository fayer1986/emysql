{application,emysql,
             [{description,"Erlang MySQL Client"},
              {vsn,"4.0"},
              {modules,[emysql,emysql_app,emysql_auth,emysql_conn,emysql_recv,
                        emysql_sup]},
              {registered,[]},
              {applications,[kernel,stdlib,sasl,crypto]},
              {env,[]},
              {mod,{emysql_app,[]}}]}.
