add_library_unity(
        duckdb_skinnerdb
        OBJECT
        test.cpp
        skinnerdb.cpp
        timer.cpp
        rl_join_order_optimizer.cpp
)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_skinnerdb>
        PARENT_SCOPE)