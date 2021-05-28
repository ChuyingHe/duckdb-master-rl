#include "duckdb.hpp"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include "imdb_constants.hpp"

using namespace duckdb;

string readFileIntoString(const string& path) {
    std::ifstream input_file(path);
    if (!input_file.is_open()) {
        std::cerr << "Could not open the file - '"<< path << "'" << std::endl;
        exit(EXIT_FAILURE);
    }
    return string((std::istreambuf_iterator<char>(input_file)), std::istreambuf_iterator<char>());
}

void loadTables(Connection con) {
    std::cout <<"\n 🌈 loadTables \n";
    for (int t = 0; t < IMDB_TABLE_COUNT; t++) {
        std::cout << IMDB_TABLE_NAMES[t] << ": ";

        con.Query(IMDB_TABLE_CREATE_SQL[t]);
        con.Query(IMDB_TABLE_FROM_CSV_SQL[t]);

        /*TODO: delet the following test code*/
        auto test = con.Query(TEST_QUERY[t]);
        test->Print();
    }
}

void addIndexes(Connection con) {
    std::cout <<"\n 🌈 addIndexes \n";
    for (int i=0; i < IMDB_TABLE_INDEX.size(); i++) {
        //std::cout << i<<"/"<< IMDB_TABLE_INDEX.size() <<" " << IMDB_TABLE_INDEX[i] << "\n";
        con.Query(IMDB_TABLE_INDEX[i]);

    }
    return;
}


void runJOBQuerys(Connection con) {
    std::cout <<"\n 🌈 runJOBQuerys \n";

    std::string path = getRootPath() + "/chuying/job-query";
    for (const auto & entry : std::filesystem::directory_iterator(path)) {
        // con.Query("PRAGMA disable_optimizer");
        con.Query("PRAGMA enable_progress_bar");
        con.Query("PRAGMA enable_profiling='json'");

		std::string job_file = entry.path().filename().string();
        std::cout <<" 📒 job_file:" << job_file <<"\n";

        std::string job_query = "PRAGMA profile_output='" + getRootPath() +"/chuying/profiling/" + job_file + ".json';";
        con.Query(job_query);

		//con.Query("SELECT * FROM info_type;");
        std::string sql_from_file = readFileIntoString(entry.path());
		std::cout << "current query: " << sql_from_file;
        auto result = con.Query(sql_from_file);
		result->Print();
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
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
    std::cout <<"🌈 main \n";
    DuckDB db(nullptr);
    Connection con(db);

    loadTables(con);
	addIndexes(con);
	runJOBQuerys(con);
}