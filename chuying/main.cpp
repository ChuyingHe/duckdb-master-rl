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
    // std::cout <<"\n 🌈 loadTables \n";
    for (int t = 0; t < IMDB_TABLE_COUNT; t++) {
        //std::cout << IMDB_TABLE_NAMES[t] << ": ";

        con.Query(IMDB_TABLE_CREATE_SQL[t]);
        con.Query(IMDB_TABLE_FROM_CSV_SQL[t]);

        /*TODO: delete the following test code*/
        auto test = con.Query(TEST_QUERY[t]);
        test->Print();
    }
}

void addIndexes(Connection con) {
    //std::cout <<"\n 🌈 addIndexes \n";
    for (int i=0; i < IMDB_TABLE_INDEX.size(); i++) {
        //std::cout << i<<"/"<< IMDB_TABLE_INDEX.size() <<" " << IMDB_TABLE_INDEX[i] << "\n";
        con.Query(IMDB_TABLE_INDEX[i]);

    }
    return;
}

void runJOBQuerys(Connection con) {
    //con.Query("PRAGMA enable_profiling='json'");        //ProgressBar uses a seperated thread for tracking purpose
    //con.Query("PRAGMA enable_progress_bar");
    //con.Query("PRAGMA enable_rl_join_order_optimizer");

    //TODO: delete this
    int count_sql = 0;

    std::string path = getRootPath() + "/chuying/job-query";
    for (const auto & entry : std::filesystem::directory_iterator(path)) {
        if (entry.path().u8string().find(".sql")!= std::string::npos) { //only take *.sql files
            count_sql++;
            std::cout<<"Progress = "<< count_sql <<"/113 \n";

            std::string job_file = entry.path().filename().string();
            std::cout <<job_file <<", ";
            /*
            std::string job_profiling = "PRAGMA profile_output='" + getRootPath() +"/chuying/profiling/" + job_file + ".json';";
            con.Query(job_profiling);
            std::cout <<"entry_path" <<entry.path() <<"\n 🎄 JOB query = " << job_query <<"\n\n";*/
            std::string job_query = readFileIntoString(entry.path());
            // Timer timer;
            auto result = con.Query(job_query);
            result->Print();
            //double duration = timer.check();
            //std::cout <<"time_total = " << duration <<"\n";
        }
    }
    //printf("end of runJOBQuerys() \n");
}

/*
#define TESTING_DIRECTORY_NAME "chuying_temp_dir"


string TestDirectoryPath() {
    FileSystem fs;
    if (!fs.DirectoryExists(TESTING_DIRECTORY_NAME)) {
        fs.CreateDirectory(TESTING_DIRECTORY_NAME);
    }
    return TESTING_DIRECTORY_NAME;
}
string TestCreatePath(string suffix) {
    FileSystem fs;
    return fs.JoinPath(TestDirectoryPath(), suffix);
}
*/

bool existDB(std::string db) {
    std::string db_dir = getBuildPath() + "/" + db ;
    //std::cout << "test = " <<db_dir << "\n";
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
    }*/

	//addIndexes(con);
	runJOBQuerys(con);

}