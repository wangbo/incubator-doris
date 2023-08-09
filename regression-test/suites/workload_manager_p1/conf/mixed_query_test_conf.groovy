global_conf.enable_test="true"
global_conf.url="jdbc:mysql://10.16.10.8:9230/hits?useSSL=false"
global_conf.username="root"
global_conf.password=""
global_conf.enable_pipe="false"
global_conf.enable_group="false"

//// <1s
tiny_query.label="tiny query"
tiny_query.dir="sql/tiny_query"
tiny_query.c="2"
tiny_query.i="20"
tiny_query.group="tiny_group"

// <5s
small_query.label="small query"
small_query.dir="sql/small_query"
small_query.c="2"
small_query.i="20"
small_query.group=""

