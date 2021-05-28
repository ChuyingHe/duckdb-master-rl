//
// Created by Chuying He on 02/05/2021.
//

#ifndef DUCKDB_IMDB_CONSTANTS_HPP
#define DUCKDB_IMDB_CONSTANTS_HPP


#include <unistd.h>
#include <string>
#include <cstring>
#include "duckdb.hpp"

std::string getBuildPath() {
    const int MAXPATH=250;
    char buffer[MAXPATH];
    getcwd(buffer, MAXPATH);
    std::string build_path = duckdb::string_t(buffer).GetString();
    std::cout << "\n build_path"<<build_path <<std::endl;

    return build_path;
}

std::string getRootPath() {
    std::string build_path = getBuildPath();
	std::vector<int> slash_pos;
    for (int i = 0; i < build_path.size(); i++) {
        if (build_path[i] == '/')  {
			slash_pos.push_back(i);
        }
    }

    std::string rootPath = duckdb::string_t(build_path).GetString().substr(0,*(slash_pos.rbegin() + 1));
    std::cout << "\n rootPath"<<rootPath <<std::endl;

    return rootPath;
}

// count for Table

const int IMDB_TABLE_COUNT = 21;

// Table name
const char *IMDB_TABLE_NAMES[] = {
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type", "company_name", "company_type", "complete_cast",
    "info_type", "keyword", "kind_type", "link_type", "movie_companies", "movie_info_idx", "movie_keyword", "movie_link",
    "name", "role_type", "title", "movie_info", "person_info"
};

// Table creation
const char *IMDB_TABLE_CREATE_SQL[] = {
    "CREATE TABLE aka_name (id integer NOT NULL PRIMARY KEY, person_id integer NOT NULL, name character varying, imdb_index character varying(3), name_pcode_cf character varying(11), name_pcode_nf character varying(11), surname_pcode character varying(11), md5sum character varying(65));",
    "CREATE TABLE aka_title (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, title character varying, imdb_index character varying(4), kind_id integer NOT NULL, production_year integer, phonetic_code character varying(5), episode_of_id integer, season_nr integer, episode_nr integer, note character varying(72), md5sum character varying(32));",
    "CREATE TABLE cast_info (id integer NOT NULL PRIMARY KEY, person_id integer NOT NULL, movie_id integer NOT NULL, person_role_id integer, note character varying, nr_order integer, role_id integer NOT NULL);",
    "CREATE TABLE char_name (id integer NOT NULL PRIMARY KEY, name character varying NOT NULL, imdb_index character varying(2), imdb_id integer, name_pcode_nf character varying(5), surname_pcode character varying(5), md5sum character varying(32));",
    "CREATE TABLE comp_cast_type (id integer NOT NULL PRIMARY KEY, kind character varying(32) NOT NULL);",
    "CREATE TABLE company_name (id integer NOT NULL PRIMARY KEY, name character varying NOT NULL, country_code character varying(6), imdb_id integer, name_pcode_nf character varying(5), name_pcode_sf character varying(5), md5sum character varying(32));",
    "CREATE TABLE company_type (id integer NOT NULL PRIMARY KEY, kind character varying(32));",
    "CREATE TABLE complete_cast (id integer NOT NULL PRIMARY KEY, movie_id integer, subject_id integer NOT NULL, status_id integer NOT NULL);",
    "CREATE TABLE info_type (id integer NOT NULL PRIMARY KEY, info character varying(32) NOT NULL);",
    "CREATE TABLE keyword (id integer NOT NULL PRIMARY KEY, keyword character varying NOT NULL, phonetic_code character varying(5));",
    "CREATE TABLE kind_type (id integer NOT NULL PRIMARY KEY, kind character varying(15));",
    "CREATE TABLE link_type (id integer NOT NULL PRIMARY KEY, link character varying(32) NOT NULL);",
    "CREATE TABLE movie_companies (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, company_id integer NOT NULL, company_type_id integer NOT NULL, note character varying);",
    "CREATE TABLE movie_info_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));",
    "CREATE TABLE movie_keyword (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, keyword_id integer NOT NULL);",
    "CREATE TABLE movie_link (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, linked_movie_id integer NOT NULL, link_type_id integer NOT NULL);",
    "CREATE TABLE name (id integer NOT NULL PRIMARY KEY, name character varying NOT NULL, imdb_index character varying(9), imdb_id integer, gender character varying(1), name_pcode_cf character varying(5), name_pcode_nf character varying(5), surname_pcode character varying(5), md5sum character varying(32));",
    "CREATE TABLE role_type (id integer NOT NULL PRIMARY KEY, role character varying(32) NOT NULL);",
    "CREATE TABLE title (id integer NOT NULL PRIMARY KEY, title character varying NOT NULL, imdb_index character varying(5), kind_id integer NOT NULL, production_year integer, imdb_id integer, phonetic_code character varying(5), episode_of_id integer, season_nr integer, episode_nr integer, series_years character varying(49), md5sum character varying(32));",
    "CREATE TABLE movie_info (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying);",
    "CREATE TABLE person_info (id integer NOT NULL PRIMARY KEY, person_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying);"
};


std::vector<std::string> IMDB_TABLE_INDEX = {
    "CREATE INDEX company_id_movie_companies ON movie_companies(company_id);",
    "CREATE INDEX company_type_id_movie_companies ON movie_companies(company_type_id);",
    "CREATE INDEX info_type_id_movie_info_idx ON movie_info_idx(info_type_id);",
    "CREATE INDEX info_type_id_movie_info ON movie_info(info_type_id);",
    "CREATE INDEX info_type_id_person_info ON person_info(info_type_id);",
    "CREATE INDEX keyword_id_movie_keyword ON movie_keyword(keyword_id);",
    "CREATE INDEX kind_id_aka_title ON aka_title(kind_id);",
    "CREATE INDEX kind_id_title ON title(kind_id);",
    "CREATE INDEX linked_movie_id_movie_link ON movie_link(linked_movie_id);",
    "CREATE INDEX link_type_id_movie_link ON movie_link(link_type_id);",
    "CREATE INDEX movie_id_aka_title ON aka_title(movie_id);",
    "CREATE INDEX movie_id_cast_info ON cast_info(movie_id);",
    "CREATE INDEX movie_id_complete_cast ON complete_cast(movie_id);",
    "CREATE INDEX movie_id_movie_companies ON movie_companies(movie_id);",
    "CREATE INDEX movie_id_movie_info_idx ON movie_info_idx(movie_id);",
    "CREATE INDEX movie_id_movie_keyword ON movie_keyword(movie_id);",
    "CREATE INDEX movie_id_movie_link ON movie_link(movie_id);",
    "CREATE INDEX movie_id_movie_info ON movie_info(movie_id);",
    "CREATE INDEX person_id_aka_name ON aka_name(person_id);",
    "CREATE INDEX person_id_cast_info ON cast_info(person_id);",
    "CREATE INDEX person_id_person_info ON person_info(person_id);",
    "CREATE INDEX person_role_id_cast_info ON cast_info(person_role_id);",
    "CREATE INDEX role_id_cast_info ON cast_info(role_id);"
};

// Table copy from csv file
const std::string IMDB_DIRECTORY = getRootPath() + "/chuying/imdb/";

const std::string IMDB_TABLE_FROM_CSV_SQL[] = {
    "COPY aka_name FROM '" + IMDB_DIRECTORY + "aka_name.csv' ESCAPE '\\';",
    "COPY aka_title FROM '" + IMDB_DIRECTORY + "aka_title.csv' ESCAPE '\\';",
    "COPY cast_info FROM '" + IMDB_DIRECTORY + "cast_info.csv' ESCAPE '\\';",
    "COPY char_name FROM '" + IMDB_DIRECTORY + "char_name.csv' ESCAPE '\\';",
    "COPY comp_cast_type FROM '" + IMDB_DIRECTORY + "comp_cast_type.csv' ESCAPE '\\';",
    "COPY company_name FROM '" + IMDB_DIRECTORY + "company_name.csv' ESCAPE '\\';",
    "COPY company_type FROM '" + IMDB_DIRECTORY + "company_type.csv' ESCAPE '\\';",
    "COPY complete_cast FROM '" + IMDB_DIRECTORY + "complete_cast.csv' ESCAPE '\\';",
    "COPY info_type FROM '" + IMDB_DIRECTORY + "info_type.csv' ESCAPE '\\';",
    "COPY keyword FROM '" + IMDB_DIRECTORY + "keyword.csv' ESCAPE '\\';",
    "COPY kind_type FROM '" + IMDB_DIRECTORY + "kind_type.csv' ESCAPE '\\';",
    "COPY link_type FROM '" + IMDB_DIRECTORY + "link_type.csv' ESCAPE '\\';",
    "COPY movie_companies FROM '" + IMDB_DIRECTORY + "movie_companies.csv' ESCAPE '\\';",
    "COPY movie_info_idx FROM '" + IMDB_DIRECTORY + "movie_info_idx.csv' ESCAPE '\\';",
    "COPY movie_keyword FROM '" + IMDB_DIRECTORY + "movie_keyword.csv' ESCAPE '\\';",
    "COPY movie_link FROM '" + IMDB_DIRECTORY + "movie_link.csv' ESCAPE '\\';",
    "COPY name FROM '" + IMDB_DIRECTORY + "name.csv' ESCAPE '\\';",
    "COPY role_type FROM '" + IMDB_DIRECTORY + "role_type.csv' ESCAPE '\\';",
    "COPY title FROM '" + IMDB_DIRECTORY + "title.csv' ESCAPE '\\';",
    "COPY movie_info FROM '" + IMDB_DIRECTORY + "movie_info.csv' ESCAPE '\\';",
    "COPY person_info FROM '" + IMDB_DIRECTORY + "person_info.csv' ESCAPE '\\';"
};

/*Test*/

const char* TEST_QUERY[] = {
    "SELECT COUNT(*) FROM aka_name;",
    "SELECT COUNT(*) FROM aka_title;",
    "SELECT COUNT(*) FROM cast_info;",
    "SELECT COUNT(*) FROM char_name;",
    "SELECT COUNT(*) FROM comp_cast_type;",
    "SELECT COUNT(*) FROM company_name;",
    "SELECT COUNT(*) FROM company_type;",
    "SELECT COUNT(*) FROM complete_cast;",
    "SELECT COUNT(*) FROM info_type;",
    "SELECT COUNT(*) FROM keyword;",
    "SELECT COUNT(*) FROM kind_type;",
    "SELECT COUNT(*) FROM link_type;",
    "SELECT COUNT(*) FROM movie_companies;",
    "SELECT COUNT(*) FROM movie_info_idx;",
    "SELECT COUNT(*) FROM movie_keyword;",
    "SELECT COUNT(*) FROM movie_link;",
    "SELECT COUNT(*) FROM name;",
    "SELECT COUNT(*) FROM role_type;",
    "SELECT COUNT(*) FROM title;",
    "SELECT COUNT(*) FROM movie_info;",
    "SELECT COUNT(*) FROM person_info;"
};

const int IMDB_QUERIES_COUNT = 114;


#endif // DUCKDB_IMDB_CONSTANTS_HPP
