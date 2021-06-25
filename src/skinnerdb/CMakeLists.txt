add_library_unity(
        duckdb_skinnerdb
        OBJECT
        test.cpp
)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_skinnerdb>
        PARENT_SCOPE)