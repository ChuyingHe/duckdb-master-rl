#include "duckdb.hpp"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <thread>

#include "imdb_constants.hpp"
#include "duckdb/skinnerdb/timer.hpp"

using namespace duckdb;
#define IMDB_DIRECTORY_NAME "duckdb_imdb"

string readFileIntoString(const string& path) {
    std::ifstream input_file(path);
    if (!input_file.is_open()) {
        std::cerr << "Could not open the file - '"<< path << "'" << std::endl;
        exit(EXIT_FAILURE);
    }
    return string((std::istreambuf_iterator<char>(input_file)), std::istreambuf_iterator<char>());
}

void loadTables(Connection con) {
    for (int t = 0; t < IMDB_TABLE_COUNT; t++) {
        con.Query(IMDB_TABLE_CREATE_SQL[t]);
        con.Query(IMDB_TABLE_FROM_CSV_SQL[t]);
        auto test = con.Query(TEST_QUERY[t]);
        test->Print();
    }
}

void addIndexes(Connection con) {
    for (int i=0; i < IMDB_TABLE_INDEX.size(); i++) {
        con.Query(IMDB_TABLE_INDEX[i]);
    }
    return;
}

void runJOBQuerys(Connection con) {
    //con.Query("PRAGMA enable_profiling='json'");        //ProgressBar uses a seperated thread for tracking purpose
    //con.Query("PRAGMA enable_progress_bar");

    int count_sql = 0;

    std::string path = getRootPath() + "/chuying/job-query";
    for (const auto & entry : std::filesystem::directory_iterator(path)) {
        if (entry.path().u8string().find(".sql")!= std::string::npos) { //only take *.sql files
            count_sql++;
            con.Query("PRAGMA enable_rl_join_order_optimizer");

            std::cout<<"Progress = "<< count_sql <<"/113 \n";

            std::string job_file = entry.path().filename().string();
            std::cout <<job_file <<",";

            std::string job_query = readFileIntoString(entry.path());

            auto result = con.Query(job_query);
            result->Print();
        }
    }
}

bool existDB(std::string db) {
    std::string db_dir = getBuildPath() + "/" + db ;
    if (std::filesystem::exists(db_dir)) {
        std::cout<< "the db already exists";
        return true;
    }
    return false;
}

int main() {
    // if persistent db exist
    auto storage_db = IMDB_DIRECTORY + "imdb";
    DuckDB db(storage_db);
    Connection con(db);

    // if persistent db not exist
    /*FileSystem fs;
    if (!fs.DirectoryExists(storage_db)) {
        printf("create persistent db \n");
        loadTables(con);
    }
    addIndexes(con);*/

    runJOBQuerys(con);

}