//
// Created by Chuying He on 02/07/2021.
//

#ifndef DUCKDB_TIMER_HPP
#define DUCKDB_TIMER_HPP

#include <chrono>
#include <iostream>

class Timer {
public:
    Timer();
    ~Timer();
    // void stop();
    double check(); //return duration

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> m_StartTimePoint;
};

#endif //DUCKDB_TIMER_HPP
